package main

import (
	"async-event-rest/internal/api/handler"
	"async-event-rest/internal/kafka/producer"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Starting API server..")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit
	log.Println("Shutdown signal received..")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Println("Server exiting...")
}
