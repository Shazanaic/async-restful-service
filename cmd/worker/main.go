package main

import (
	"async-event-rest/internal/kafka/consumer"
	"async-event-rest/internal/models"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func worker(id int, jobs <-chan kafka.Message, db *postgres.Repository, cache *repoRedis.Repository) {
	for msg := range jobs {
		var event models.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[Worker %d] Error unmarshaling JSON: %v", id, err)
			continue
		}

		log.Printf("[Worker %d] Started processing UserID=%d, Type=%s", id, event.UserID, event.Type)

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

		log.Printf("[Worker %d] Finished processing UserID=%d", id, event.UserID)
	}
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

	jobs := make(chan kafka.Message, 100)

	numWorkers := 5
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, db, cache)
	}

	for {
		msg, err := kafkaConsumer.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		jobs <- msg

	}
}
