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

func (e EmailUser) Perform(attempt int32) error {
	log.Printf("emailing %v, attempt=%v", e.Email, attempt)
	return nil
}

func (e EmailUser) Load(data string) (pgjobs.Job, error) {
	var n EmailUser
	err := json.Unmarshal([]byte(data), &n)
	return n, err
}
