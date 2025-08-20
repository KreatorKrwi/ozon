package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	ch *redis.Client
}

func NewCache(ch *redis.Client) *Cache {
	return &Cache{ch: ch}
}

func (c *Cache) GetData(orderUID string) (string, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	val, err := c.ch.Get(ctx, orderUID).Result()
	if err != nil {
		return "", err
	}
	return val, err
}

func (c *Cache) SetData(json []byte, orderUID string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := c.ch.Set(ctx, orderUID, json, 1*time.Hour).Err(); err != nil {
		return err
	}
	return nil
}
