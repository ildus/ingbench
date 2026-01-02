package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/ildus/ingres"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	DbName string
	Scale  int
}

type BenchConfig struct {
	Config
	Clients      int
	Transactions int
	Duration     int
	SelectOnly   bool
}

type BenchResult struct {
	TotalTxns     int64
	TotalDuration time.Duration
	LatencySum    int64
	LatencyCount  int64
	MinLatency    int64
	MaxLatency    int64
}

func createTables(db *sql.DB) error {
	schemas := []string{
		`CREATE TABLE bench_branches (
			bid INT,
			bbalance INT,
			filler CHAR(88)
		)`,
		`CREATE TABLE bench_tellers (
			trid INT,
			bid INT,
			tbalance INT,
			filler CHAR(84)
		)`,
		`CREATE TABLE bench_accounts (
			aid INT,
			bid INT,
			abalance INT,
			filler CHAR(84)
		)`,
		`CREATE TABLE bench_history (
			trid INT,
			bid INT,
			aid INT,
			delta INT,
			mtime TIMESTAMP,
			filler CHAR(22)
		)`,
	}

	for _, schema := range schemas {
		if _, err := db.Exec(schema); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal("Program is exiting with exception: ", err)
		}
	}()

	initCmd := flag.NewFlagSet("init", flag.ExitOnError)
	scale := initCmd.Int("scale", 1, "scaling factor")
	dbname := initCmd.String("dbname", "mydb", "database name")

	benchCmd := flag.NewFlagSet("bench", flag.ExitOnError)
	benchClients := benchCmd.Int("clients", 1, "number of concurrent clients")
	benchTxns := benchCmd.Int("transactions", 0, "number of transactions per client (0 = unlimited)")
	benchDuration := benchCmd.Int("time", 10, "duration in seconds")
	benchDBName := benchCmd.String("dbname", "mydb", "database name")
	benchScale := benchCmd.Int("scale", 1, "scaling factor (for determining account range)")
	benchSelectOnly := benchCmd.Bool("select-only", false, "run select-only benchmark")

	if len(os.Args) < 2 {
		fmt.Println("Usage: ingbench <command> [options]")
		fmt.Println("Commands:")
		fmt.Println("  init    Initialize database with test data")
		fmt.Println("  bench   Run benchmark")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initCmd.Parse(os.Args[2:])
		cfg := Config{
			DbName: *dbname,
			Scale:  *scale,
		}
		if err := initDB(cfg); err != nil {
			log.Fatalf("Initialization failed: %v", err)
		}
	case "bench":
		benchCmd.Parse(os.Args[2:])
		cfg := BenchConfig{
			Config: Config{
				DbName: *benchDBName,
				Scale:  *benchScale,
			},
			Clients:      *benchClients,
			Transactions: *benchTxns,
			Duration:     *benchDuration,
			SelectOnly:   *benchSelectOnly,
		}
		if err := runBench(cfg); err != nil {
			log.Fatalf("Benchmark failed: %v", err)
		}

	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func dropTables(db *sql.DB) error {
	tables := []string{"bench_history", "bench_tellers", "bench_accounts", "bench_branches"}
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", table, err)
		}
	}
	return nil
}

func initDB(cfg Config) error {
	db, err := sql.Open("ingres", cfg.DbName)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("dropping old tables...")
	if err := dropTables(db); err != nil {
		return err
	}

	fmt.Println("creating tables...")
	if err := createTables(db); err != nil {
		return err
	}

	fmt.Printf("generating data (scale=%d)...\n", cfg.Scale)
	if err := generateData(db, cfg.Scale); err != nil {
		return err
	}

	fmt.Println("done.")
	return nil
}

func generateData(db *sql.DB, scale int) error {
	nBranches := scale
	nTellers := scale * 10
	nAccounts := scale * 20000

	fmt.Printf("generating %d branches...\n", nBranches)
	for i := 1; i <= nBranches; i++ {
		_, err := db.Exec("INSERT INTO bench_branches (bid, bbalance, filler) VALUES ( ~V , 0, '')", i)
		if err != nil {
			return fmt.Errorf("failed to insert branch: %w", err)
		}
	}

	fmt.Printf("generating %d tellers...\n", nTellers)
	for i := 1; i <= nTellers; i++ {
		bid := (i-1)/10 + 1
		_, err := db.Exec("INSERT INTO bench_tellers (trid, bid, tbalance, filler) VALUES ( ~V , ~V , 0, '')", i, bid)
		if err != nil {
			return fmt.Errorf("failed to insert teller: %w", err)
		}
	}

	fmt.Printf("generating %d accounts...\n", nAccounts)
	batchSize := 1000
	for i := 1; i <= nAccounts; i += batchSize {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		stmt, err := tx.Prepare("INSERT INTO bench_accounts (aid, bid, abalance, filler) VALUES ( ~V , ~V , 0, '')")
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare statement: %w", err)
		}

		for j := 0; j < batchSize && (i+j) <= nAccounts; j++ {
			aid := i + j
			bid := (aid-1)/100000 + 1
			if _, err := stmt.Exec(aid, bid); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("failed to insert account: %w", err)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		if i%10000 == 1 {
			fmt.Printf("  %d of %d\n", i, nAccounts)
		}
	}

	fmt.Println("creating indexes...")
	indexes := []string{
		"CREATE INDEX (i1 ON bench_tellers (bid))",
		"CREATE INDEX (i2 ON bench_accounts (bid))",
	}
	for _, idx := range indexes {
		if _, err := db.Exec(idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

func runBench(cfg BenchConfig) error {
	db, err := sql.Open("ingres", cfg.DbName)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(cfg.Clients)
	db.SetMaxIdleConns(cfg.Clients)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	nAccounts := cfg.Scale * 100000
	nBranches := cfg.Scale
	nTellers := cfg.Scale * 10

	fmt.Printf("starting benchmark...\n")
	fmt.Printf("clients: %d\n", cfg.Clients)
	fmt.Printf("duration: %d s\n", cfg.Duration)
	if cfg.SelectOnly {
		fmt.Println("mode: select-only")
	} else {
		fmt.Println("mode: TPC-B (sort of)")
	}

	var totalTxns atomic.Int64
	var latencySum atomic.Int64
	var latencyCount atomic.Int64
	var minLatency atomic.Int64
	var maxLatency atomic.Int64
	var failedQueries atomic.Int64
	minLatency.Store(1<<63 - 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Duration)*time.Second)
	defer cancel()

	startTime := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

			txnCount := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if cfg.Transactions > 0 && txnCount >= cfg.Transactions {
					return
				}

				txnStart := time.Now()
				var err error
				if cfg.SelectOnly {
					err = runSelectOnlyTxn(db, rng, nAccounts)
				} else {
					err = runTpcbTxn(db, rng, nAccounts, nBranches, nTellers)
				}
				latency := time.Since(txnStart).Microseconds()

				if err != nil {
					failedQueries.Add(1)
					continue
				}

				totalTxns.Add(1)
				latencySum.Add(latency)
				latencyCount.Add(1)
				txnCount++

				for {
					oldMin := minLatency.Load()
					if latency >= oldMin || minLatency.CompareAndSwap(oldMin, latency) {
						break
					}
				}

				for {
					oldMax := maxLatency.Load()
					if latency <= oldMax || maxLatency.CompareAndSwap(oldMax, latency) {
						break
					}
				}
			}
		}(i)
	}

	progressTicker := time.NewTicker(time.Second)
	defer progressTicker.Stop()
	lastTxns := int64(0)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-progressTicker.C:
				current := totalTxns.Load()
				fmt.Printf("progress: %.1f s, %.1f tps\n",
					time.Since(startTime).Seconds(),
					float64(current-lastTxns))
				lastTxns = current
			}
		}
	}()

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\nbenchmark results:")
	txns := totalTxns.Load()
	fmt.Printf("number of transactions: %d\n", txns)
	fmt.Printf("failed queries: %d\n", failedQueries.Load())
	fmt.Printf("duration: %.3f s\n", duration.Seconds())
	fmt.Printf("tps: %.2f (including connections establishing)\n", float64(txns)/duration.Seconds())

	if latencyCount.Load() > 0 {
		avgLatency := float64(latencySum.Load()) / float64(latencyCount.Load())
		fmt.Printf("latency average: %.3f ms\n", avgLatency/1000.0)
		fmt.Printf("latency min: %.3f ms\n", float64(minLatency.Load())/1000.0)
		fmt.Printf("latency max: %.3f ms\n", float64(maxLatency.Load())/1000.0)
	}

	return nil
}

func runSelectOnlyTxn(db *sql.DB, rng *rand.Rand, nAccounts int) error {
	aid := rng.Intn(nAccounts) + 1
	var abalance int
	err := db.QueryRow("SELECT abalance FROM bench_accounts WHERE aid = ~V ", aid).Scan(&abalance)
	return err
}

func runTpcbTxn(db *sql.DB, rng *rand.Rand, nAccounts, nBranches, nTellers int) error {
	aid := rng.Intn(nAccounts) + 1
	bid := rng.Intn(nBranches) + 1
	trid := rng.Intn(nTellers) + 1
	delta := rng.Intn(2000) - 1000

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE bench_accounts SET abalance = abalance + ~V WHERE aid = ~V ", delta, aid)
	if err != nil {
		return err
	}

	_, err = tx.Exec("SELECT abalance FROM bench_accounts WHERE aid = ~V ", aid)
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE bench_tellers SET tbalance = tbalance + ~V WHERE trid = ~V ", delta, trid)
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE bench_branches SET bbalance = bbalance + ~V WHERE bid = ~V ", delta, bid)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO bench_history (trid, bid, aid, delta, mtime) VALUES ( ~V , ~V , ~V , ~V , CURRENT_TIMESTAMP)",
		trid, bid, aid, delta)
	if err != nil {
		return err
	}

	return tx.Commit()
}
