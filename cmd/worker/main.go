package main

import (
	"async-event-rest/internal/kafka/consumer"
	"async-event-rest/internal/models"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func worker(id int, jobs <-chan kafka.Message, db *postgres.Repository, cache *repoRedis.Repository, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range jobs {
		var event models.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[Worker %d] Error unmarshaling JSON: %v", id, err)
			continue
		}

		log.Printf("[Worker %d] Processing UserID=%d, Type=%s", id, event.UserID, event.Type)

		if err := db.SaveEvent(event); err != nil {
			log.Printf("[Worker %d] Failed to save event: %v", id, err)
			continue
		}

		if err := db.UpdateStats(event.UserID, event.Type); err != nil {
			log.Printf("[Worker %d] Failed to update stats: %v", id, err)
			continue
		}

		if err := cache.InvalidateCache(context.Background(), event.UserID); err != nil {
			log.Printf("[Worker %d] Failed to invalidate cache: %v", id, err)
			continue
		}
	}
	log.Printf("[Worker: %d] Shutted down.", id)
}

func main() {
	connStr := "host=localhost port=5432 user=postgres password=secret dbname=events_db sslmode=disable"

	db, err := postgres.New(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Connected to PostgreSQL successfully!")

	cache, err := repoRedis.New("localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis.")
	}
	log.Println("Connected to Redis successfully!")

	kafkaConsumer := consumer.New("127.0.0.1:9092", "user-events", "worker-group")
	defer kafkaConsumer.Close()
	log.Println("Connected to Kafka(consumer) successfully! Starting workers..")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	jobs := make(chan kafka.Message, 100)

	numWorkers := 5
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, db, cache, &wg)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutdown signal received..")
		cancel()
	}()

	for {
		msg, err := kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		jobs <- msg
	}

	close(jobs)
	wg.Wait()

	log.Println("All workers are shutted down. Extiting...")
}
