package pgjobs

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	JOB_STATUS_SCHEDULED = "new"
	JOB_STATUS_FINISHED  = "finished"
	JOB_STATUS_FAILED    = "failed"

	MAX_RETRY = 3
)

var PollInterval = 1 * time.Second
var JobsTableName = "_jobs"

type Job interface {
	Perform(attempt int32) error
	Load(data string) (Job, error)
}

type jobRaw struct {
	Id       int64
	TypeName string
	Status   string
	Queue    string
	Data     string
	Error    string
	Attempt  int32

	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

type JobQueue struct {
	db *sql.DB

	mutex        sync.Mutex
	typeRegistry map[string]reflect.Type
}

func NewQueue(db *sql.DB) *JobQueue {
	return &JobQueue{
		db:           db,
		typeRegistry: make(map[string]reflect.Type),
	}
}

func (j *JobQueue) SetupSchema(ctx context.Context) error {
	schema := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %v (
        id serial primary key,

        type_name text NOT NULL,
        status text NOT NULL,
        queue text NOT NULL,
        data text NOT NULL,

        error text,
        attempt int default 0,

        created_at    timestamp not null default now(),
        scheduled_at  timestamp,
        started_at    timestamp,
        finished_at   timestamp
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_status ON %v(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_queue ON %v(queue);`, JobsTableName, JobsTableName, JobsTableName)
	_, err := j.db.ExecContext(ctx, schema)
	return err
}

func (j *JobQueue) EnqueueAt(ctx context.Context, job Job, queue string, at time.Time) error {
	typeName := j.typeName(job)

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed marshaling: %v", err)
	}

	if _, err = j.db.ExecContext(
		ctx,
		`INSERT INTO `+JobsTableName+` (type_name, status, queue, data, scheduled_at) VALUES ($1, $2, $3, $4, $5)`,
		typeName,
		JOB_STATUS_SCHEDULED,
		queue,
		data,
		at,
	); err != nil {
		return fmt.Errorf("queue: failed inserting job: %w", err)
	}

	return nil
}

func (j *JobQueue) Enqueue(ctx context.Context, job Job, queue string) error {
	log.Printf("queue: enqueing queue=%v job=%+v", queue, job)

	typeName := j.typeName(job)

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed marshaling: %v", err)
	}

	if _, err = j.db.ExecContext(
		ctx,
		`INSERT INTO `+JobsTableName+` (type_name, status, queue, data) VALUES ($1, $2, $3, $4)`,
		typeName,
		JOB_STATUS_SCHEDULED,
		queue,
		data,
	); err != nil {
		return fmt.Errorf("queue: failed inserting job: %w", err)
	}

	return nil
}

func (j *JobQueue) Dequeue(ctx context.Context, queues []string) error {
	var job jobRaw

	sqlStmt := ` 
        UPDATE
          ` + JobsTableName + `
        SET
          status = $1,
          started_at = now(),
          attempt = attempt + 1
        WHERE
          id IN (
            SELECT
              id FROM ` + JobsTableName + ` j
            WHERE
              (j.status = $2 or (j.status = $3 and j.attempt < $4))
              AND j.queue = any($5)
              AND j.type_name = any($6) 
              AND (j.scheduled_at is null or (j.scheduled_at <= now()))
            ORDER BY
              j.scheduled_at, j.created_at
            FOR UPDATE SKIP LOCKED
          LIMIT 1)
        RETURNING id, type_name, data, attempt
    `

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	queueArray, err := pqArray(queues)
	if err != nil {
		return err
	}

	typesArray, err := pqArray(mapKeys(j.typeRegistry))
	if err != nil {
		return err
	}

	row := tx.QueryRowContext(
		ctx,
		sqlStmt,
		JOB_STATUS_FINISHED,
		JOB_STATUS_SCHEDULED,
		JOB_STATUS_FAILED,
		MAX_RETRY,
		queueArray,
		typesArray,
	)
	err = row.Scan(&job.Id, &job.TypeName, &job.Data, &job.Attempt)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	// get original go type based on type name
	jobType, err := j.getType(job.TypeName)
	if err != nil {
		_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = NOW(), error = $3 WHERE id = $2`, JOB_STATUS_FAILED, job.Id, err.Error())
		if err != nil {
			return fmt.Errorf("unable to exec error for failed job", err)
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("unable to commit error for failed job", err)
		}

		return fmt.Errorf("unable to find related job '%v': %v", job.TypeName, err)
	}

	// create a new object by unmarshaling the job data
	loadedJob, err := jobType.Load(job.Data)
	if err != nil {
		return err
	}

	// execute job
	err = loadedJob.Perform(int32(job.Attempt))
	if err != nil {
		// TODO: add retry handling and save error to job row
		_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = NOW(), error = $3 WHERE id = $2`, JOB_STATUS_FAILED, job.Id, err.Error())
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = NOW() WHERE id = $2`, JOB_STATUS_FINISHED, job.Id)
	if err != nil {
		return fmt.Errorf("failed updating job status: %w", err)
	}

	return tx.Commit()
}

func (j *JobQueue) Worker(ctx context.Context, queues []string, types ...interface{}) error {
	// register all passed types in a type type registry.
	// this allows to map job types back to their corresponding go type
	// to execute the Perform() action.
	for _, t := range types {
		j.registerType(t)
	}

	tm := time.NewTicker(PollInterval)
	defer tm.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tm.C:
			if err := j.Dequeue(ctx, queues); err != nil {
				log.Println("queue: dequeue failed", err)
			}
		}
	}
}

func (j *JobQueue) typeName(typedNil interface{}) string {
	name := reflect.TypeOf(typedNil).String()
	if strings.HasPrefix(name, "*") {
		name = name[1:]
	}

	return name
}

func (j *JobQueue) registerType(typedNil interface{}) {
	t := reflect.TypeOf(typedNil).Elem()
	name := j.typeName(typedNil)

	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.typeRegistry[name] = t
}

func (j *JobQueue) getType(name string) (Job, error) {
	item, ok := j.typeRegistry[name]

	if !ok {
		return nil, fmt.Errorf("type not found in type registry. did you register the job?")
	}

	t := reflect.New(item).Elem().Interface().(Job)

	return t, nil
}

// pqArray and appendArrayQuotedBytes func extracted from https://github.com/lib/pq
// to remove dependency on lib/pq
func pqArray(a []string) (string, error) {
	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+3*n)
		b[0] = '{'

		b = appendArrayQuotedBytes(b, []byte(a[0]))
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = appendArrayQuotedBytes(b, []byte(a[i]))
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

func appendArrayQuotedBytes(b, v []byte) []byte {
	b = append(b, '"')
	for {
		i := bytes.IndexAny(v, `"\`)
		if i < 0 {
			b = append(b, v...)
			break
		}
		if i > 0 {
			b = append(b, v[:i]...)
		}
		b = append(b, '\\', v[i])
		v = v[i+1:]
	}
	return append(b, '"')
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
