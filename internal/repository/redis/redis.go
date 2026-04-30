package redis

import (
	"async-event-rest/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Repository struct {
	client *redis.Client
}

func New(addr string) (*Repository, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return &Repository{client: client}, nil
}

func (rep *Repository) GetStats(ctx context.Context, userID int) (models.UserStats, error) {
	key := fmt.Sprintf("user:%d:stats", userID)

	val, err := rep.client.Get(ctx, key).Result()
	if err != nil {
		return models.UserStats{}, err
	}

	var stats models.UserStats

	err = json.Unmarshal([]byte(val), &stats)
	return stats, err
}

func (rep *Repository) SetStats(ctx context.Context, userID int, stats models.UserStats) error {
	key := fmt.Sprintf("user:%d:stats", userID)

	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	return rep.client.Set(ctx, key, data, 5*time.Minute).Err()
}

func (rep *Repository) InvalidateCache(ctx context.Context, userID int) error {
	key := fmt.Sprintf("user:%d:stats", userID)

	return rep.client.Del(ctx, key).Err()
}
