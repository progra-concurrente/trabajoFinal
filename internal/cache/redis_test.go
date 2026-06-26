package cache

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRedisJSONCache(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not configured")
	}
	ctx := context.Background()
	client, err := Open(ctx, address, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetJSON(ctx, "test:json", map[string]int{"value": 42}, time.Minute); err != nil {
		t.Fatal(err)
	}
	var actual map[string]int
	found, err := client.GetJSON(ctx, "test:json", &actual)
	if err != nil || !found || actual["value"] != 42 {
		t.Fatalf("unexpected cache result found=%v value=%v err=%v", found, actual, err)
	}
}
