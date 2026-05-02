package main

import (
	"async-event-rest/internal/api/handler"
	"async-event-rest/internal/kafka/producer"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"log"
	"net/http"
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

	kafkaProducer := producer.New("127.0.0.1:9092", "user-events")
	defer kafkaProducer.Close()
	log.Println("Connected to Kafka(producer) successfully!")

	h := handler.New(db, cache, kafkaProducer)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/events", h.CreateEvent)
	mux.HandleFunc("/stats", h.GetStats)

	log.Println("Starting API server..")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
