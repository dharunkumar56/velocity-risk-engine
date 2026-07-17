# Velocity Tracker

An ultra-low-latency, highly concurrent **Fraud Risk Management (FRM)** pipeline designed to ingest, process, and evaluate financial transaction velocity limits in real time.

Built with **Go**, **Apache Kafka**, and **Redis**, the system leverages an event-driven architecture and a **Redis Lua-powered sliding window evaluation engine** to process financial transactions at massive throughput while maintaining strong consistency and fault tolerance.

---

# 🚀 Performance Highlights

- **80,000+ Evaluation TPS** (Kafka → Redis evaluation pipeline)
- **23,598 End-to-End RPS** (Complete ingestion-to-evaluation lifecycle)
- **3.2 Million Transactions** processed during benchmark testing
- **40,000 Concurrent Users** simulated

---

# 🏗️ Architecture

The pipeline follows an asynchronous event-driven architecture that removes synchronous bottlenecks and allows downstream services to scale independently under sustained high traffic.

```text
                 +----------------------+
                 |  Load Test Client    |
                 | (3.2M HTTP Requests) |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Ingestion API        |
                 | Stateless HTTP       |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Apache Kafka         |
                 | 32 Partitions        |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Risk Engine Workers  |
                 | 128 Goroutines       |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Redis                |
                 | Lua Sliding Window   |
                 | Sorted Sets (ZSET)   |
                 +----------------------+
```

---

# 🧩 Components

## Load Generator (`velocity-tracker-client`)

A high-performance benchmarking client that simulates over **40,000 unique users** generating millions of asynchronous payment requests. It measures end-to-end throughput under sustained production-scale traffic.

---

## Ingestion Gateway (`velocity-tracker-ingestion`)

A lightweight stateless HTTP gateway responsible for:

- Receiving transaction requests
- Validating request payloads
- Parsing incoming data
- Routing events to Kafka using `user_id` as the partition key

Because the service is stateless, multiple ingestion nodes can be horizontally scaled behind a load balancer.

---

## Apache Kafka

Kafka acts as the durable event backbone.

Key characteristics:

- **32 partitions**
- `user_id` used as the message key
- Strict ordering maintained for every individual account
- Massive parallelism across different users
- Durable message persistence

---

## Risk Engine (`velocity-tracker-engine`)

A highly concurrent processing service responsible for:

- Continuously consuming Kafka partitions
- Running **128 concurrent worker goroutines**
- Redis pipelining
- Atomic Lua-powered velocity evaluation
- Manual Kafka offset management

---

# 🧠 Design Optimizations & Enterprise Fault Tolerance

## 1. Atomic Sliding Window Evaluation

Velocity checks execute entirely inside Redis using a localized Lua script.

Each evaluation atomically performs:

- `ZREMRANGEBYSCORE`
- `ZADD`
- `ZCARD`
- `EXPIRE`

Bundling these operations into a single Redis transaction guarantees:

- Atomic execution
- Strong consistency
- Zero race conditions
- No distributed application locks
- Safe concurrent execution across all 128 workers

---

## 2. Reliable Lifecycle Management (Graceful Shutdown)

To prevent:

- dropped transactions
- half-processed requests
- broken network sockets

the engine uses explicit context propagation.

Workers consume Kafka using:

```go
reader.FetchMessage(ctx)
```

Upon receiving `SIGINT` or `SIGTERM`:

- Context cancellation propagates through all workers
- In-flight requests finish processing
- Redis operations complete
- Kafka offsets commit successfully
- Workers exit cleanly within milliseconds

---

## 3. At-Least-Once Delivery with Manual Commits

Automatic offset commits were intentionally disabled.

Instead, offsets are committed only after Redis evaluation succeeds.

```go
reader.CommitMessages(ctx, msg)
```

Benefits:

- No message loss
- At-least-once delivery guarantees
- Safe recovery after unexpected crashes
- Transactions are automatically replayed if a worker fails before committing

---

## 4. Dead-Letter Queue (DLQ)

Malformed JSON or schema violations should never stop the processing pipeline.

If deserialization fails:

1. Error is logged
2. Original payload is forwarded to

```
payment-events-dlq
```

This isolates poison-pill messages while allowing healthy traffic to continue uninterrupted.

---

## 5. Highly Concurrent Worker Pool

The risk engine maximizes throughput using:

- **128 worker goroutines**
- Redis connection pool (`PoolSize = 300`)
- Redis pipelining
- Kafka batch fetching (`MinBytes = 100 KB`)

These optimizations significantly reduce:

- Network round trips
- Context switches
- CPU scheduling overhead

resulting in sustained high throughput during heavy traffic bursts.

---

# 📊 Performance Benchmarks

| Metric | Result |
|---------|--------|
| Total Transactions | **3,200,000** |
| Concurrent Users | **40,000** |
| Kafka Partitions | **32** |
| Risk Workers | **128** |
| Evaluation Pipeline | **80,000+ TPS** |
| End-to-End Pipeline | **23,598 RPS** |

---

# 🚀 Getting Started

## Prerequisites

- Go **1.22+**
- Docker
- Docker Compose

---

## 1. Start Infrastructure

```bash
cd velocity-tracker-infra

docker compose down -v
docker compose up -d
```

---

## 2. Create Kafka Topic

Wait approximately **5 seconds** for Kafka to initialize.

```bash
docker exec -it velocity-kafka \
kafka-topics \
--bootstrap-server localhost:9092 \
--create \
--topic payment-events \
--partitions 32 \
--replication-factor 1
```

---

## 3. Start Services

Open three separate terminals.

### Risk Engine

```bash
cd velocity-tracker-engine
go run main.go
```

### Ingestion Gateway

```bash
cd velocity-tracker-ingestion
go run main.go
```

### Load Generator

```bash
cd velocity-tracker-client
go run main.go
```

---

# 🛠️ Technology Stack

| Category | Technology |
|----------|------------|
| Language | Go |
| Message Broker | Apache Kafka |
| Storage | Redis |
| Redis Features | Lua Scripting, Sorted Sets (ZSET), Pipelining |
| Containerization | Docker |
| Orchestration | Docker Compose |

---

# 🎯 Key Features

- Event-driven asynchronous architecture
- Stateless ingestion layer
- Atomic Redis Lua evaluation
- Sliding window fraud detection
- Kafka partition ordering per user
- 128 concurrent worker pool
- Redis pipelining
- Manual Kafka offset commits
- At-least-once delivery semantics
- Graceful shutdown handling
- Dead-letter queue isolation
- Production-scale benchmarking
- Horizontal scalability
- High-throughput low-latency processing

---

## License

This project is provided for educational and portfolio purposes.