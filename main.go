package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Read environment variables directly
	host := os.Getenv("POSTGRES_HOST")
	portStr := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	// Read timing configuration
	insertIntervalStr := os.Getenv("INSERT_INTERVAL_SECONDS")
	cleanupIntervalStr := os.Getenv("CLEANUP_INTERVAL_SECONDS")
	maxLogAgeStr := os.Getenv("MAX_LOG_AGE_SECONDS")

	// Convert port to int
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatal("Invalid port number")
	}

	insertInterval, err := strconv.ParseFloat(insertIntervalStr, 64)
	if err != nil || insertInterval <= 0 {
		insertInterval = 5.0 // Default: 5 seconds
	}

	cleanupInterval, err := strconv.ParseFloat(cleanupIntervalStr, 64)
	if err != nil || cleanupInterval <= 0 {
		cleanupInterval = 60.0 // Default: 60 seconds
	}

	maxLogAge, err := strconv.Atoi(maxLogAgeStr)
	if err != nil || maxLogAge <= 0 {
		maxLogAge = 30 // Default: 30 seconds
	}

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Insert interval: %.1f seconds\n", insertInterval)
	fmt.Printf("  Cleanup interval: %.1f seconds\n", cleanupInterval)
	fmt.Printf("  Max log age: %d seconds\n\n", maxLogAge)

	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)

	fmt.Println("Connection string:", psqlInfo)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Cannot connect to database:", err)
	}
	fmt.Println("Successfully connected to database!")

	// Create table if it doesn't exist
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS audit_logs (
			id SERIAL PRIMARY KEY,
			message TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_created_at ON audit_logs(created_at);
	`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("Table ready!")

	// Start goroutine to insert audit logs every 5 seconds
	go insertAuditLogsRoutine(db, insertInterval)

	// Start goroutine to delete old records every minute
	go cleanupOldRecordsRoutine(db, cleanupInterval, maxLogAge)

	// Keep the program running
	fmt.Println("Audit log system started. Press Ctrl+C to stop.")
	select {} // Block forever
}

func postToDB(db *sql.DB, message string) {
	query := `
        INSERT INTO audit_logs (message, created_at)
        VALUES ($1, $2)
        RETURNING id, created_at
    `

	var id int
	var createdAt time.Time
	err := db.QueryRow(query, message, time.Now()).Scan(&id, &createdAt)
	if err != nil {
		log.Fatalf("Failed to insert audit log: %v", err)
	}

	fmt.Printf("Inserted: ID=%d, Message=%s, Time=%v\n", id, message, createdAt)
}

func deleteOldRecords(db *sql.DB, secondsOld int) {
	cutoffTime := time.Now().Add(-time.Duration(secondsOld) * time.Second)

	// Delete in batches of 5 to reduce database load
	batchSize := 5
	totalDeleted := 0

	for {
		query := `
			DELETE FROM audit_logs 
			WHERE id IN (
				SELECT id FROM audit_logs 
				WHERE created_at < $1 
				ORDER BY created_at ASC 
				LIMIT $2
			)
			RETURNING id, message, created_at
		`

		rows, err := db.Query(query, cutoffTime, batchSize)
		if err != nil {
			log.Printf("Error deleting: %v", err)
			return
		}

		var deletedIDs []int
		var deletedCount int

		for rows.Next() {
			var id int
			var message string
			var createdAt time.Time

			err := rows.Scan(&id, &message, &createdAt)
			if err != nil {
				log.Printf("Error scanning: %v", err)
				continue
			}

			deletedIDs = append(deletedIDs, id)
			deletedCount++
			fmt.Printf("    - ID=%d, Message=%s, Created=%v\n", id, message, createdAt.Format("15:04:05"))
		}
		rows.Close()

		if deletedCount == 0 {
			break // No more records to delete
		}

		totalDeleted += deletedCount
		fmt.Printf("  Deleted batch of %d records (IDs: %v)\n", deletedCount, deletedIDs)

		// Small pause between batches to avoid overwhelming the database
		time.Sleep(1000 * time.Millisecond)

	}

	if totalDeleted > 0 {
		fmt.Printf("✓ Deleted %d total records older than %d seconds\n", totalDeleted, secondsOld)
	} else {
		fmt.Printf("✓ No records older than %d seconds to delete\n", secondsOld)
	}
}

func insertAuditLogsRoutine(db *sql.DB, intervalSeconds float64) {
	counter := 1
	ticker := time.NewTicker(time.Duration(intervalSeconds*1000) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		message := fmt.Sprintf("Audit log #%d", counter)
		postToDB(db, message)
		counter++
	}
}

func cleanupOldRecordsRoutine(db *sql.DB, intervalSeconds float64, maxAgeSeconds int) {
	ticker := time.NewTicker(time.Duration(intervalSeconds*1000) * time.Millisecond)
	defer ticker.Stop()

	var mu sync.Mutex
	isRunning := false

	for range ticker.C {
		mu.Lock()
		if isRunning {
			fmt.Println("⚠️  Previous cleanup still running, skipping this cycle")
			mu.Unlock()
			continue
		}
		isRunning = true
		mu.Unlock()

		fmt.Println("\n--- Running cleanup job ---")
		deleteOldRecords(db, maxAgeSeconds)

		mu.Lock()
		isRunning = false
		mu.Unlock()
	}
}
