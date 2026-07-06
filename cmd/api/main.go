package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"powersight/internal/api"
	"powersight/internal/auth"
	"powersight/internal/cache"
	"powersight/internal/cluster"
	"powersight/internal/realtime"
	"powersight/internal/store"
	"powersight/pkg/ml"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := openMongo(ctx, env("MONGO_URI", "mongodb://localhost:27017"), env("MONGO_DATABASE", "powersight"))
	defer db.Close(context.Background())
	redis := openRedis(ctx, env("REDIS_ADDR", "localhost:6379"), env("REDIS_PASSWORD", ""), envInt("REDIS_DB", 0))
	defer redis.Close()
	hub := realtime.New()

	initial, err := ml.LoadModel(env("MODEL_PATH", "data/processed/forecast_model.json"))
	if err != nil {
		log.Fatalf("load initial model: %v", err)
	}
	initial, err = db.EnsureInitialModel(ctx, initial)
	if err != nil {
		log.Fatalf("seed initial model: %v", err)
	}
	if err := db.EnsureAdminUser(ctx, env("API_USERNAME", "admin"), env("API_PASSWORD", "powersight")); err != nil {
		log.Fatalf("seed admin user: %v", err)
	}

	eventSink := func(ctx context.Context, event cluster.Event) {
		db.SaveClusterEvent(ctx, event)
		hub.Broadcast("cluster."+event.Type, event)
		_ = redis.PublishJSON(ctx, "powersight:events", map[string]any{"type": "cluster." + event.Type, "payload": event})
	}
	coordinator := cluster.New(
		env("CLUSTER_TCP_ADDR", ":9000"),
		envDuration("NODE_REQUEST_TIMEOUT", 10*time.Second),
		envDuration("HEARTBEAT_INTERVAL", 3*time.Second),
		envInt("MISSED_HEARTBEATS", 3),
		eventSink,
	)
	if err := coordinator.Start(); err != nil {
		log.Fatalf("start cluster coordinator: %v", err)
	}
	defer coordinator.Close()

	authService := auth.New(
		env("JWT_SECRET", "change-this-development-secret"),
		envDuration("JWT_TTL", 8*time.Hour),
	)
	server := api.New(db, redis, coordinator, authService, hub, initial,
		env("SUSTAINABILITY_REPORT_PATH", "data/processed/sustainability_report.json"),
		env("OPENAPI_PATH", "docs/openapi.yaml"), env("APP_TIMEZONE", "America/Lima"),
		envInt("BATCH_CONCURRENCY", 8),
	)
	httpServer := &http.Server{
		Addr: env("HTTP_ADDR", ":8080"), Handler: server.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	go func() {
		log.Printf("HTTP API %s, Swagger /swagger/, WebSocket /ws, cluster %s", httpServer.Addr, coordinator.Addr())
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func openMongo(ctx context.Context, uri, database string) *store.Store {
	for attempt := 1; ; attempt++ {
		db, err := store.Open(ctx, uri, database)
		if err == nil {
			return db
		}
		log.Printf("MongoDB not ready (attempt %d): %v", attempt, err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		}
	}
}

func openRedis(ctx context.Context, address, password string, database int) *cache.Cache {
	for attempt := 1; ; attempt++ {
		client, err := cache.Open(ctx, address, password, database)
		if err == nil {
			return client
		}
		log.Printf("Redis not ready (attempt %d): %v", attempt, err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}
func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}
