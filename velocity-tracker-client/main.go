package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	UserID        string  `json:"user_id"`
	DeviceID      string  `json:"device_id"`
	Amount        float64 `json:"amount"`
	Timestamp     int64   `json:"timestamp"`
}

const (
	TargetURL        = "http://localhost:8080/api/v1/payments"
	GeneratorThreads = 32     // Broad parallel thread engines
	WorkersPerThread = 25     // 32 * 25 = 800 parallel persistent connections flooding the system
	EventsPerThread  = 100000 // Target footprint: 3.2 Million requests
)

var (
	client      *http.Client
	globalCount uint64
)

func init() {
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        3000,
			MaxIdleConnsPerHost: 3000, // Crucial to prevent socket blocking on local test runs
			IdleConnTimeout:     90 * time.Second,
			MaxConnsPerHost:     5000,
		},
		Timeout: 10 * time.Second,
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Printf("🔥 Deploying High-Sustained Traffic Engine targeting %s...\n", TargetURL)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < GeneratorThreads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			executeSynchronousLoad(id)
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	finalRPS := float64(globalCount) / duration.Seconds()

	fmt.Printf("\n✅ Wave Complete! Pushed %d transactions in %v. Realized Processing Rate: %.2f RPS\n",
		globalCount, duration, finalRPS)
}

func executeSynchronousLoad(threadID int) {
	txChannel := make(chan Transaction, 5000)
	var workerWg sync.WaitGroup

	for i := 0; i < WorkersPerThread; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for tx := range txChannel {
				sendSynchronousPayload(tx)
				atomic.AddUint64(&globalCount, 1)
			}
		}()
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))

	for i := 0; i < EventsPerThread; i++ {
		txChannel <- Transaction{
			TransactionID: fmt.Sprintf("tx_t%d_%d", threadID, i),
			UserID:        fmt.Sprintf("usr_%d", r.Intn(40000)),
			DeviceID:      fmt.Sprintf("dev_%d", r.Intn(20000)),
			Amount:        float64(r.Intn(3000) + 1),
			Timestamp:     time.Now().UnixMilli(),
		}
	}

	close(txChannel)
	workerWg.Wait()
}

func sendSynchronousPayload(tx Transaction) {
	payload, _ := json.Marshal(tx)
	req, err := http.NewRequest("POST", TargetURL, bytes.NewBuffer(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close() // Clear body buffer instantly to recycle connection line
}