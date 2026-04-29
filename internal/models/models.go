package models

import "time"

type Event struct {
	ID        int       `json:"id"`
	UserID    int       `json:"user_id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

type UserStats struct {
	UserID      int `json:"user_id"`
	LoginCount  int `json:"login_count"`
	ActionCount int `json:"action_count"`
}
