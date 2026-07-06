package auth

import (
	"testing"
	"time"
)

func TestLoginAndParseJWT(t *testing.T) {
	service := New("test-secret", time.Hour)
	token, err := service.Issue("admin", "admin")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := service.Parse(token)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Username != "admin" || identity.Role != "admin" {
		t.Fatalf("unexpected identity %+v", identity)
	}
	if _, err := service.Issue("", "user"); err == nil {
		t.Fatal("expected username validation")
	}
}
