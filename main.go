package main

import (
	"auditlog-cleaner/config"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var methods = []string{"POST", "GET", "DELETE", "PUT", "PATCH"}

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

	// Reset return error
	resetTable(db)

	// Create root table
	createRootTable(db)

	// Start goroutines
	go insertAuditLogsRoutine(db, cfg.Timing.InsertIntervalSeconds, cfg.Timing.InsertAmountOfLogs)
	go cleanupRoutine(db, cfg.Timing.CleanupIntervalSeconds, cfg.Timing.MaxLogAgeSeconds)

	fmt.Println("Audit log cleaner started. CTRL+C to stop.")
	select {}
}

// ---------------------------------------------------------
// DB SETUP
// ---------------------------------------------------------
func resetTable(db *sql.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS audit_logs CASCADE;`)
	if err != nil {
		log.Fatal("Drop error:", err)
	}
	fmt.Println("✓ Dropped old audit_logs table")
}

func createRootTable(db *sql.DB) {
	query := `
		CREATE TABLE audit_logs (
			id BIGSERIAL,
			method TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY (id, created_at)
		) PARTITION BY RANGE (created_at);
	`

	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("Failed creating parent table:", err)
	}
	fmt.Println("✓ Created partitioned audit_logs parent table")
}

// ---------------------------------------------------------
// PARTITIONS
// ---------------------------------------------------------
func ensurePartition(db *sql.DB, t time.Time) error {
	start := t.Truncate(time.Minute) // ex: 12:05:00
	end := start.Add(time.Minute)    // ex: 12:06:00
	name := fmt.Sprintf("audit_logs_%s", start.Format("20060102_1504"))

	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF audit_logs
		FOR VALUES FROM ('%s') TO ('%s');
	`,
		name,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"))

	_, err := db.Exec(stmt)
	if err == nil {
		fmt.Printf("✓ Partition ensured: %s\n", name)
	}
	return err
}

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
// INSERT ROUTINE
// ---------------------------------------------------------
func insertAuditLogsRoutine(db *sql.DB, everySeconds float64, amountOfLogs int) {
	ticker := time.NewTicker(time.Duration(everySeconds * float64(time.Second)))
	defer ticker.Stop()

	for range ticker.C {
		if err := writeAuditLogsToDbBatch(db, amountOfLogs); err != nil {
			fmt.Println("Batch insert failed:", err)
		} else {
			fmt.Printf("✓ Inserted %d logs in batch\n", amountOfLogs)
		}
	}
}

// func writeAuditLogsToDbBatch add x amount of logs (batch)
func writeAuditLogsToDbBatch(db *sql.DB, amountOfLogs int) error {
	now := time.Now()

	// Ensure partition exists BEFORE starting transaction
	if err := ensurePartition(db, now); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO audit_logs (method, created_at) VALUES ($1, $2)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < amountOfLogs; i++ {
		method := methods[rand.Intn(len(methods))]
		_, err := stmt.Exec(method, now)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// func writeAuditLogsToDbPerSecondBatch (uses)
// Not used
// func writeAuditLogsToDbPerSecondBatch(db *sql.DB, amountOfLogs int) {
// 	ticker := time.NewTicker(1 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		if err := writeAuditLogsToDbBatch(db, amountOfLogs); err != nil {
// 			fmt.Println("Batch insert failed:", err)
// 		} else {
// 			fmt.Printf("Inserted %d logs in batch\n", amountOfLogs)
// 		}
// 	}
// }

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
