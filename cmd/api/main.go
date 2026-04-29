package main

import (
	"async-event-rest/internal/api/handler"
	"async-event-rest/internal/repository/postgres"
	"log"
	"net/http"
)

func main() {
	connStr := "host=localhost port=5432 user=postgres password=secret dbname=events_db sslmode=disable"

	repo, err := postgres.New(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Connected to PostgreSQL successfully!")

	h := handler.New(repo)

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
