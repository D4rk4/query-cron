package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/mileusna/crontab"

	//	"github.com/Masterminds/sprig/v3"
	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB
var ctab *crontab.Crontab
var DSN, SQL string

func main() {
	DSN := os.Getenv("DSN")
	config := os.Getenv("CRONTAB")
	// Check if DSN is set
	if DSN == "" {
		fmt.Println("DSN env is not set, using default")
		DSN = "root@tcp(localhost:3306)/example"
		fmt.Println("DSN is set to", DSN)
	}
	// Check if CONFIG is set
	if config == "" {
		// Check if crontab exists
		if _, err := os.Stat("crontab"); os.IsNotExist(err) {
			panic("CONFIG env is not set and crontab does not exist")
		} else {
			config = "crontab"
		}
	}
	// Try to connect to database
	DB, err := sql.Open("mysql", DSN)
	if err != nil {
		fmt.Println("Wrong settings:", DSN)
		panic(err)
	} else {
		DB.Close()
	}

	// Create crontab
	ctab := crontab.New()
	// Load crontab
	jobs, err := os.Open(config)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(jobs)
	now := time.Now()
	fmt.Println(now, "Loading crons from", config)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if line[0] == '#' {
			continue
		}
		// Split line by tab to get schedule and SQL
		schedule := strings.Split(line, "\t")[0]
		SQL := strings.Split(line, "\t")[1]
		now := time.Now()
		fmt.Println(now, "Adding job", schedule, SQL)
		// Add job to crontab
		ctab.MustAddJob(schedule, SQLJob, DSN, SQL)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctab.Clear()

}

func SQLJob(DSN, SQL string) {
	now := time.Now()
	// Try to connect to database
	DB, err := sql.Open("mysql", DSN)
	if err != nil {
		fmt.Println(err)
	}
	// Execute SQL job and log output
	stmt, err := DB.Prepare(SQL)
	if err != nil {
		fmt.Println(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		fmt.Println(err)
	} else {
		// Count rows
		var count int
		for rows.Next() {
			count++
		}
		// Log SQL job
		fmt.Println(now, "Executed SQL job \"", SQL, "\" and got", count, "rows")

	}

}
