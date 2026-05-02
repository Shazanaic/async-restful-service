package handler

import (
	"async-event-rest/internal/kafka/producer"
	"async-event-rest/internal/models"
	"async-event-rest/internal/repository/postgres"
	repoRedis "async-event-rest/internal/repository/redis"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

type Handler struct {
	db       *postgres.Repository
	cache    *repoRedis.Repository
	producer *producer.Producer
}

func New(repo *postgres.Repository, cache *repoRedis.Repository, p *producer.Producer) *Handler {
	return &Handler{db: repo, cache: cache, producer: p}
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event models.Event

	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event.CreatedAt = time.Now()

	// if err := h.db.SaveEvent(event); err != nil {
	// 	http.Error(w, "Failed to save event", http.StatusInternalServerError)
	// 	return
	// }

	// if err := h.db.UpdateStats(event.UserID, event.Type); err != nil {
	// 	http.Error(w, "Failed to update stats", http.StatusInternalServerError)
	// 	return
	// }

	// if err := h.cache.InvalidateCache(r.Context(), event.UserID); err != nil {
	// 	log.Printf("Failed to clear cache of uID: %d: %v", event.UserID, err)
	// }

	if err := h.producer.ProduceEvent(r.Context(), event); err != nil {
		http.Error(w, "Failed to produce event to Kafka", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status":"event recorded"}`))
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userIDStr := r.URL.Query().Get("user_id")
	if userIDStr == "" {
		http.Error(w, "Missing user_id parameter", http.StatusBadRequest)
		return
	}

	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		http.Error(w, "Invalid user_id", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	stats, err := h.cache.GetStats(r.Context(), userID)
	if err == nil {
		log.Println("Cache used!!!")
		json.NewEncoder(w).Encode(stats)
		return
	}

	stats, err = h.db.GetStats(userID)
	if err != nil {
		http.Error(w, "Stats not found in PostgreSQL", http.StatusNotFound)
		return
	}

	if err := h.cache.SetStats(r.Context(), userID, stats); err != nil {
		log.Printf("Failed to save cache for uID: %d: %v", userID, err)
	}

	log.Println("Cache was not used!!!")
	json.NewEncoder(w).Encode(stats)
}
