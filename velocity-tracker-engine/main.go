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
	KafkaDLQTopic = "payment-events-dlq"
	ConsumerGroup = "fraud-evaluation-group"
	RedisAddr     = "localhost:6379"
)

var (
	rdb         *redis.Client
	dlqWriter   *kafka.Writer
	totalBreaches int64
)

var slidingWindowLua = redis.NewScript(`
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window_start = tonumber(ARGV[2])
    local tx_id = ARGV[3]
    local ttl = tonumber(ARGV[4])

    redis.call('ZRemRangeByScore', key, '-inf', window_start)
    redis.call('ZAdd', key, now, tx_id .. ':' .. now)
    local count = redis.call('ZCard', key)
    redis.call('Expire', key, ttl)

    return count
`)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	workerCount := 128

	rdb = redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		PoolSize:     300,
		MinIdleConns: 50,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})

	// Initialize the synchronous Dead Letter Queue fallback writer
	dlqWriter = &kafka.Writer{
		Addr:         kafka.TCP(KafkaBroker),
		Topic:        KafkaDLQTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	defer dlqWriter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("❌ Redis link broken: %v", err)
	}
	cancel()

	// Handle graceful system shutdown contexts cleanly
	shutdownCtx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	fmt.Printf("🚀 Risk Engine spinning up %d parallel evaluation workers...\n", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runEngineWorker(shutdownCtx, workerID)
		}(i)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Gracefully stepping down Evaluation cluster...")
	stop() // Broad signal cascading downwards to all goroutines
	wg.Wait()
	_ = rdb.Close()
	
	fmt.Printf("\n📊 [FINAL METRIC] Total unique users successfully flagged: %d\n", atomic.LoadInt64(&totalBreaches))
}

func runEngineWorker(ctx context.Context, workerID int) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{KafkaBroker},
		GroupID:        ConsumerGroup,
		Topic:          KafkaTopic,
		MinBytes:       100e3, 
		MaxBytes:       50e6,
		CommitInterval: 0, // Disable auto-commit to handle manual lifecycle acknowledgement
	})
	defer reader.Close()

	for {
		// 1. Explicitly check context status BEFORE invoking blocking I/O calls
		if ctx.Err() != nil {
			return
		}

		// 2. Pass context directly into FetchMessage so it wakes up immediately upon shutdown signals
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return // Context cancelled or connection explicitly closed
		}

		var tx Transaction
		if err := json.Unmarshal(msg.Value, &tx); err != nil {
			// ROUTE TO DLQ: Move corrupted messages out of the primary pipeline layout
			handleCorruptMessage(ctx, msg.Value, err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		evaluateVelocityStrict(ctx, tx)
		
		// Commit transaction offset only after processing pipeline successfully handles it
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[Worker %d] Failed to commit message offset: %v", workerID, err)
		}
	}
}

func evaluateVelocityStrict(ctx context.Context, tx Transaction) {
	redisKey := fmt.Sprintf("velocity:%s", tx.UserID)
	now := tx.Timestamp
	sixtySecsAgo := now - 60000
	ttlSeconds := 300

	// Propagated context prevents script hanging if the application context fires done
	res, err := slidingWindowLua.Run(ctx, rdb, []string{redisKey}, now, sixtySecsAgo, tx.TransactionID, ttlSeconds).Result()
	if err != nil {
		return
	}

	count := res.(int64)
	if count == 6 {
		currentTotal := atomic.AddInt64(&totalBreaches, 1)
		if currentTotal%5000 == 0 {
			fmt.Printf("🛡️  [SYSTEM HEALTHY] Total unique users flagged so far: %d / 40000\n", currentTotal)
		}
	}
}

func handleCorruptMessage(ctx context.Context, rawPayload []byte, parseErr error) {
	log.Printf("⚠️  Corrupt transaction schema skipped. Redirecting event log to DLQ. Error: %v", parseErr)
	
	err := dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("err_%d", time.Now().UnixNano())),
		Value: rawPayload,
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("❌ Critical failure routing to Dead Letter Queue: %v", err)
	}
}