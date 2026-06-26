package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct{ client *redis.Client }

func Open(ctx context.Context, address, password string, database int) (*Cache, error) {
	client := redis.NewClient(&redis.Options{Addr: address, Password: password, DB: database})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Cache{client: client}, nil
}

func (c *Cache) Close()                         { _ = c.client.Close() }
func (c *Cache) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }

func (c *Cache) GetJSON(ctx context.Context, key string, target any) (bool, error) {
	value, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(value, target)
}

func (c *Cache) SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

func (c *Cache) PublishJSON(ctx context.Context, channel string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Publish(ctx, channel, data).Err()
}
