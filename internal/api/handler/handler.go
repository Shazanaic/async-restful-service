package handler

import (
	"async-event-rest/internal/models"
	"async-event-rest/internal/repository/postgres"
	"encoding/json"
	"net/http"
	"strconv"
)

type Handler struct {
	repo *postgres.Repository
}

func New(repo *postgres.Repository) *Handler {
	return &Handler{repo: repo}
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

	if err := h.repo.SaveEvent(event); err != nil {
		http.Error(w, "Failed to save event", http.StatusInternalServerError)
		return
	}

	if err := h.repo.UpdateStats(event.UserID, event.Type); err != nil {
		http.Error(w, "Failed to update stats", http.StatusInternalServerError)
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

	stats, err := h.repo.GetStats(userID)
	if err != nil {
		http.Error(w, "Stats not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
