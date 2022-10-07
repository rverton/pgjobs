package pgjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/lib/pq"
)

const (
	JOB_STATUS_SCHEDULED  = "new"
	JOB_STATUS_PROCESSING = "processing"
	JOB_STATUS_FINISHED   = "finished"
	JOB_STATUS_FAILED     = "failed"

	MAX_RETRY = 3
)

var PollInterval = 1 * time.Second

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

	typeRegistry map[string]reflect.Type
}

func NewQueue(db *sql.DB) *JobQueue {
	return &JobQueue{
		db:           db,
		typeRegistry: make(map[string]reflect.Type),
	}
}

func (j *JobQueue) SetupSchema(ctx context.Context) error {
	schema := `
    CREATE TABLE IF NOT EXISTS jobs (
        id serial primary key,

        type_name text NOT NULL,
        status text NOT NULL,
        queue text NOT NULL,
        data text NOT NULL,

        error text,
        attempt int default 0,

        created_at  timestamp not null default now(),
        started_at  timestamp,
        finished_at timestamp
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue);`
	_, err := j.db.ExecContext(ctx, schema)
	return err
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
		`INSERT INTO jobs (type_name, status, queue, data) VALUES ($1, $2, $3, $4)`,
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
	log.Printf("queue: dequeuing queues=%v", queues)

	var job jobRaw

	sqlStmt := ` 
        UPDATE
          jobs
        SET
          status = $1,
          started_at = now(),
          attempt = attempt + 1
        WHERE
          id IN (
            SELECT
              id FROM jobs j
            WHERE
              j.status = $2 or (j.status = $3 and j.attempt < $4)
              and j.queue = any($5)
            ORDER BY
              j.created_at
            FOR UPDATE SKIP LOCKED
          LIMIT 1)
        RETURNING id, type_name, data, attempt
    `

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		sqlStmt,
		JOB_STATUS_FINISHED,
		JOB_STATUS_SCHEDULED,
		JOB_STATUS_FAILED,
		MAX_RETRY,
		pq.Array(queues),
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
		return fmt.Errorf("unable to find related job task: %v", err)
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
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = $1, finished_at = NOW(), error = $3 WHERE id = $2`, JOB_STATUS_FAILED, job.Id, err.Error())
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = $1, finished_at = NOW() WHERE id = $2`, JOB_STATUS_FINISHED, job.Id)
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

	for {
		if err := j.Dequeue(ctx, queues); err != nil {
			log.Printf("queue: dequeue failed: %v", err)
		}
		time.Sleep(PollInterval)
	}

}

func (j *JobQueue) typeName(typedNil interface{}) string {
	t := reflect.TypeOf(typedNil).Elem()
	return t.PkgPath() + "." + t.Name()
}

func (j *JobQueue) registerType(typedNil interface{}) {
	t := reflect.TypeOf(typedNil).Elem()
	name := j.typeName(typedNil)
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
