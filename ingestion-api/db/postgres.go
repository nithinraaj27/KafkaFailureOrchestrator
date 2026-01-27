package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is capitalized so it is "exported" and visible to the handlers package
var DB *pgxpool.Pool

func Connect() error {
	// 1. Get the full URL from the environment
	dsn := os.Getenv("DB_URL")

	// Fallback for local development if DB_URL isn't set
	if dsn == "" {
		dsn = "postgres://orchestrator:orchestrator@localhost:5433/orchestrator_db?sslmode=disable"
	}

	fmt.Printf("📡 Attempting connection via DB_URL...\n")

	// 2. Set a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 3. Create the connection pool
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	// 4. Ping to verify
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	DB = pool
	log.Println("✅ Successfully connected to PostgreSQL")
	return nil
}
