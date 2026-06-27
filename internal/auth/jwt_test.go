package auth

import (
	"testing"
	"time"
)

func TestLoginAndParseJWT(t *testing.T) {
	service := New("test-secret", "admin", "password", time.Hour)
	token, err := service.Login("admin", "password")
	if err != nil {
		t.Fatal(err)
	}
	username, err := service.Parse(token)
	if err != nil {
		t.Fatal(err)
	}
	if username != "admin" {
		t.Fatalf("unexpected username %q", username)
	}
	if _, err := service.Login("admin", "wrong"); err == nil {
		t.Fatal("expected invalid credentials")
	}
}
