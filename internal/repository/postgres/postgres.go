package postgres

import (
	"async-event-rest/internal/models"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Repository struct {
	db *sql.DB
}

func (r *Repository) createTables() error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		user_id INT NOT NULL,
		type VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS user_stats (
		user_id INT PRIMARY KEY,
		login_count INT DEFAULT 0,
		action_count INT DEFAULT 0
	);`

	_, err := r.db.Exec(query)
	return err
}

func New(connStr string) (*Repository, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	repo := &Repository{db: db}

	if err := repo.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return repo, nil
}

func (r *Repository) SaveEvent(event models.Event) error {
	query := `INSERT INTO events (user_id, type) VALUES ($1, $2)`
	_, err := r.db.Exec(query, event.UserID, event.Type)
	return err
}

func (r *Repository) UpdateStats(userID int, eventType string) error {
	query := `
	INSERT INTO user_stats (user_id, login_count, action_count)
	VALUES ($1, $2, $3)
	ON CONFLICT (user_id) DO UPDATE SET
		login_count = user_stats.login_count + EXCLUDED.login_count,
		action_count = user_stats.action_count + EXCLUDED.action_count;
	`

	loginIncr := 0
	actionIncr := 0

	if eventType == "login" {
		loginIncr = 1
	} else {
		actionIncr = 1
	}

	_, err := r.db.Exec(query, userID, loginIncr, actionIncr)
	return err
}

func (r *Repository) GetStats(userID int) (models.UserStats, error) {
	query := `SELECT user_id, login_count, action_count FROM user_stats WHERE user_id = $1`

	var stats models.UserStats
	row := r.db.QueryRow(query, userID)
	err := row.Scan(&stats.UserID, &stats.LoginCount, &stats.ActionCount)

	return stats, err
}
