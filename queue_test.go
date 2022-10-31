package pgjobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var err error
	dbUrl := os.Getenv("DB_URL_TEST")
	if dbUrl == "" {
		fmt.Fprintf(os.Stderr, "DB_URL_TEST not set")
		os.Exit(1)
	}

	db, err = sql.Open("postgres", dbUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to test DB: %v", err)
		os.Exit(1)
	}

	// ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + JobsTableName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to remove previous jobs table: %v", err)
		os.Exit(1)
	}

	// create initial schema
	if err := NewQueue(db).SetupSchema(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "unable to create schema: %v", err)
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}

func TestEnqueue(t *testing.T) {
	queue := NewQueue(db)
	job := NewEmailUser("foo@example.com")

	// enequeue an example job for immediate execution
	if err := queue.Enqueue(context.Background(), job, "default"); err != nil {
		t.Errorf("failed enqueueing: %v", err)
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + JobsTableName + "").Scan(&cnt); err != nil {
		t.Fatalf("error retrieving job: %v", err)
	}

	if cnt != 1 {
		t.Fatal("no job enqueued")
	}
}

// dummy job for testing
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
	fmt.Printf("emailing %v, attempt=%v", e.Email, attempt)
	return nil
}

// this is boilerplate code and does not need to be modified
func (e EmailUser) Load(data string) (Job, error) {
	var n EmailUser
	err := json.Unmarshal([]byte(data), &n)
	return n, err
}
