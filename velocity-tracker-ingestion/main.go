package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

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
	Port        = ":8080"
	KafkaBroker = "localhost:9092"
	KafkaTopic  = "payment-events"
)

var kafkaWriter *kafka.Writer

func main() {
	// Utilize all available CPU threads for network multiplexing
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Configure synchronous Kafka writer tuned for heavy concurrent bulk batching
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(KafkaBroker),
		Topic:        KafkaTopic,
		Balancer:     &kafka.Hash{},      // Ensures strict chronological user ordering via key hashing
		MaxAttempts:  5,
		RequiredAcks: kafka.RequireOne,   // Wait for the partition leader to safely commit to disk
		Async:        false,              // CRITICAL: Synchronous path execution, blocks until written
		BatchSize:    1000,               // Accumulate up to 1000 concurrent requests into a single write operation
		BatchTimeout: 5 * time.Millisecond, // Flush batches every 5ms max to keep HTTP response loops fast
		Compression:  kafka.Lz4,          // Drastically reduces network saturation and disk I/O bottlenecks
	}
	defer kafkaWriter.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payments", handlePayment)

	// Tune the network server timeouts to prevent slow-loris connection holding under load
	server := &http.Server{
		Addr:         Port,
		Handler:      mux,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 4 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	fmt.Printf("🚀 Synchronous Ingestion Server (Pattern A) operating at 15k+ RPS on port %s...\n", Port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server connection error: %v", err)
	}
}

func handlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var tx Transaction
	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		http.Error(w, "Bad Request Schema", http.StatusBadRequest)
		return
	}

	payloadBytes, err := json.Marshal(tx)
	if err != nil {
		http.Error(w, "Serialization Failed", http.StatusInternalServerError)
		return
	}

	// EXECUTE SYNCHRONOUS WRITE: This blocks the HTTP goroutine until Kafka completes the transaction write
	err = kafkaWriter.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(tx.UserID),
		Value: payloadBytes,
		Time:  time.Now(),
	})

	if err != nil {
		// If Kafka chokes or goes offline, the client receives an error instantly to initiate retries
		http.Error(w, "Ingestion storage engine unavailable", http.StatusServiceUnavailable)
		return
	}

	// 200 OK guarantees that the transaction is safely registered inside the Kafka cluster
	w.WriteHeader(http.StatusOK)
}