package main

import (
	"context"
	"database/sql"
	"example/jobs"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rverton/pgjobs"
)

func main() {
	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// initiate queue with postgres connection
	queue := pgjobs.NewQueue(db)
	if err := queue.SetupSchema(ctx); err != nil {
		panic(err)
	}

	// example to enqueue a job
	job := jobs.NewEmailUser("foo@example.com")
	if err = queue.Enqueue(context.Background(), job, "default"); err != nil {
		log.Printf("error enqueueing: %v", err)
	}

	// start worker and pass all processable jobs
	queues := []string{"default"}
	if err := queue.Worker(ctx, queues, &jobs.EmailUser{}); err != nil {
		log.Println(err)
	}
}
