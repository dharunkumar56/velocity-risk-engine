package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	UserID        string  `json:"user_id"`
	DeviceID      string  `json:"device_id"`
	Amount        float64 `json:"amount"`
	Timestamp     int64   `json:"timestamp"`
}

const (
	KafkaBroker   = "localhost:9092"
	KafkaTopic    = "payment-events"
	ConsumerGroup = "fraud-evaluation-group"
	RedisAddr     = "localhost:6379"
)

var rdb *redis.Client

// Global atomic counter tracking the total number of unique users flagged
var totalBreaches int64

// Redis Lua script ensuring textually atomic sliding window velocity evaluation
var slidingWindowLua = redis.NewScript(`
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window_start = tonumber(ARGV[2])
    local tx_id = ARGV[3]
    local ttl = tonumber(ARGV[4])

    -- Remove stale elements
    redis.call('ZRemRangeByScore', key, '-inf', window_start)
    -- Add current event
    redis.call('ZAdd', key, now, tx_id .. ':' .. now)
    -- Count current elements
    local count = redis.call('ZCard', key)
    -- Set TTL
    redis.call('Expire', key, ttl)

    return count
`)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	workerCount := 128 // Massive concurrency to absorb network roundtrip times

	rdb = redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		PoolSize:     300, // Large connection pool to handle the active worker thread count
		MinIdleConns: 50,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("❌ Redis link broken: %v", err)
	}
	cancel()

	shutdownCtx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	fmt.Printf("🚀 Risk Engine spinning up %d parallel evaluation workers...\n", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runEngineWorker(shutdownCtx)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Gracefully stepping down Evaluation cluster...")
	stop()
	wg.Wait()
	_ = rdb.Close()
	
	// Print a clear final verification summary on exit
	fmt.Printf("\n📊 [FINAL METRIC] Total unique users successfully flagged: %d\n", atomic.LoadInt64(&totalBreaches))
}

func runEngineWorker(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{KafkaBroker},
		GroupID:        ConsumerGroup,
		Topic:          KafkaTopic,
		MinBytes:       100e3, // Fetch large 100KB chunks to optimize consumer network transfers
		MaxBytes:       50e6,
		CommitInterval: 500 * time.Millisecond,
	})
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			var tx Transaction
			if err := json.Unmarshal(msg.Value, &tx); err != nil {
				continue
			}

			evaluateVelocityStrict(tx)
		}
	}
}

func evaluateVelocityStrict(tx Transaction) {
	redisKey := fmt.Sprintf("velocity:%s", tx.UserID)
	now := tx.Timestamp
	sixtySecsAgo := now - 60000
	ttlSeconds := 300

	// Execute via bulletproof distributed Lua runner
	res, err := slidingWindowLua.Run(context.Background(), rdb, []string{redisKey}, now, sixtySecsAgo, tx.TransactionID, ttlSeconds).Result()
	if err != nil {
		return
	}

	count := res.(int64)
	if count == 6 {
		// Increment the counter safely across all goroutines
		currentTotal := atomic.AddInt64(&totalBreaches, 1)

		// Throttled logging: Only print once every 5,000 unique breaches
		if currentTotal%5000 == 0 {
			fmt.Printf("🛡️  [SYSTEM HEALTHY] Total unique users flagged so far: %d / 40000\n", currentTotal)
		}
	}
}