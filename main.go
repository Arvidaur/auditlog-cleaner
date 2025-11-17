package main

import (
	"auditlog-cleaner/config"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// ---------------------------------------------------------
// MAIN
// ---------------------------------------------------------
func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Error loading configuration:", err)
	}

	cfg.Print()

	db, err := sql.Open("postgres", cfg.Database.ConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("Cannot connect to database:", err)
	}
	fmt.Println("✓ Connected to database")

	// Start cleanup routine
	go cleanupRoutine(db, cfg.Timing.CleanupIntervalSeconds, cfg.Timing.MaxLogAgeSeconds)

	fmt.Println("Audit log partition cleaner started. CTRL+C to stop.")
	select {}
}

// ---------------------------------------------------------
// PARTITIONS
// ---------------------------------------------------------
func dropOldPartitions(db *sql.DB, olderThanSeconds int) {
	cutoff := time.Now().Add(-time.Duration(olderThanSeconds) * time.Second)

	partitionToDrop := fmt.Sprintf("audit_logs_%s", cutoff.Format("20060102_1504"))

	stmt := fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE;`, partitionToDrop)

	_, err := db.Exec(stmt)
	if err == nil {
		fmt.Printf("✓ Dropped old partition: %s\n", partitionToDrop)
	}
}

// ---------------------------------------------------------
// CLEANUP ROUTINE
// ---------------------------------------------------------
func cleanupRoutine(db *sql.DB, everySeconds float64, maxAgeSeconds int) {
	ticker := time.NewTicker(time.Duration(everySeconds * float64(time.Second)))
	defer ticker.Stop()

	var mu sync.Mutex
	isRunning := false

	for range ticker.C {
		mu.Lock()
		if isRunning {
			fmt.Println("Cleanup already running—skipping")
			mu.Unlock()
			continue
		}
		isRunning = true
		mu.Unlock()

		fmt.Println("\n--- Running cleanup job ---")

		dropOldPartitions(db, maxAgeSeconds)

		mu.Lock()
		isRunning = false
		mu.Unlock()
	}
}
