package jobs

import (
	"encoding/json"
	"log"
	"pgjobs"
)

type EmailUser struct {
	Domain string
}

func NewEnumerateDomain(domain string) *EmailUser {
	return &EmailUser{
		Domain: domain,
	}
}

func (e EmailUser) Perform(attempt int32) error {
	log.Printf("emailing %v, attempt=%v", e.Domain, attempt)
	return nil
}

func (e EmailUser) Load(data string) (pgjobs.Job, error) {
	var n EmailUser
	err := json.Unmarshal([]byte(data), &n)
	return n, err
}
