package main

import (
	"async-event-rest/internal/kafka/consumer"
	"async-event-rest/internal/models"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"context"
	"encoding/json"
	"log"
)

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
	log.Println("Connected to Kafka(consumer) successfully!")

	for {
		msg, err := kafkaConsumer.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var event models.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling JSON: %v", err)
			continue
		}

		log.Printf("Received event from Kafka: UserID=%d, Type=%s", event.UserID, event.Type)

		if err := db.SaveEvent(event); err != nil {
			log.Printf("Failed to save event to DB: %v", err)
			continue
		}

		if err := db.UpdateStats(event.UserID, event.Type); err != nil {
			log.Printf("Failed to update stats: %v", err)
			continue
		}

		if err := cache.InvalidateCache(context.Background(), event.UserID); err != nil {
			log.Printf("Failed to invalidate cache: %v", err)
			continue
		}

		log.Printf("Successfully processed event for UserID=%d\n", event.UserID)
	}
}
