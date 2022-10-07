# pgjobs, a dead simple postgres job queueing mechanism

This project aims to be a blueprint for your own job queue solution with Go and PostgreSQL.
It is recommended to fork this project and adjust the job queue to your own needs.

To create a fast queue, Postgres `SKIP LOCKED` feature is used. The technique is 
described [here](https://robinverton.de/blog/queueing-with-postgresql-and-go).

## Example usage

A complete, runnable examples can be found under `./example/`.

First define a `pgjobs.Job`, for example in `./jobs/emailUser.go`:

```go
package jobs

import (
	"encoding/json"
	"log"

	"github.com/rverton/pgjobs"
)

type EmailUser struct {
	Email string
}

func NewEmailUser(email string) *EmailUser {
	return &EmailUser{
		Email: email,
	}
}

// the action which should be executed
func (e EmailUser) Perform(attempt int32) error {
	log.Printf("emailing %v, attempt=%v", e.Email, attempt)
	return nil
}

// this is boilerplate code and does not need to be modified
func (e EmailUser) Load(data string) (pgjobs.Job, error) {
	var n EmailUser
	err := json.Unmarshal([]byte(data), &n)
	return n, err
}
```

You can then setup a queue, (optionally) enforce the jobs table schema, and work on queued jobs.

```go
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

	// enequeue an example job
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
```

## ToDo

* [ ] Remove `github.com/lib/pq` dependency
* [ ] Implement `attempt` handling
* [ ] Add error handling and retries?
