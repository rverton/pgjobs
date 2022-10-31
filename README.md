# pgjobs, a dead simple postgres job queueing mechanism

This project aims to be a blueprint for your own job queue solution with Go and
PostgreSQL. It is recommended to fork this project and adjust the job queue with
features to your own needs.

By using Postgres `SKIP LOCKED` feature, this allows to make a performant,
non-blocking and independent queue. The technique is described
[here](https://robinverton.de/blog/queueing-with-postgresql-and-go) and [here](https://www.crunchydata.com/blog/message-queuing-using-native-postgresql).

* Performat: Non-blocking queue mechanism
* Robust: Jobs will be 'freed' again when a worker crashes
* Failed jobs will be retried until `MAX_RETRIES`, current attempt is passed as argument
* Schedule jobs for later execution with `EnqueueAt(ctx, job, "queuename", timeAt)`
* Support for multiple queues
* Zero dependency

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

	job := jobs.NewEmailUser("foo@example.com")
    
	// enequeue an example job for immediate execution
	if err = queue.Enqueue(context.Background(), job, "default"); err != nil {
		log.Printf("error enqueueing: %v", err)
	}
    
	// enequeue an example job for execution in 10s+
	if err = queue.EnqueueAt(context.Background(), job, "default", time.Now().Add(10*time.Second)); err != nil {
		log.Printf("error enqueueing: %v", err)
	}

	// start worker and pass all processable jobs
	queues := []string{"default"}
	if err := queue.Worker(ctx, queues, &jobs.EmailUser{}); err != nil {
		log.Println(err)
	}
}
```

## Configuration

* The polling interval for workers can be adjusted via `pgjobs.PollInterval`.

## ToDo

* [X] Make job processing more robust by using a transaction
* [X] Implement `attempt` handling
* [X] Add error handling and retries?
* [X] Add scheduled execution
* [ ] Remove `github.com/lib/pq` dependency
* [ ] Add priority queuing
