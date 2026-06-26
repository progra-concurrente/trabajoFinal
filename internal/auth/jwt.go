package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userKey contextKey = "user"

type Service struct {
	secret   []byte
	username string
	password string
	ttl      time.Duration
}

type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

func New(secret, username, password string, ttl time.Duration) *Service {
	return &Service{secret: []byte(secret), username: username, password: password, ttl: ttl}
}

func (s *Service) Login(username, password string) (string, error) {
	if username != s.username || password != s.password {
		return "", errors.New("invalid credentials")
	}
	now := time.Now()
	claims := Claims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: username, IssuedAt: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

func (s *Service) Parse(tokenValue string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenValue, &Claims{}, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, errors.New("unexpected signing method")
		}
		return s.secret, nil
	})
	if err != nil || !token.Valid {
		return "", errors.New("invalid or expired token")
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || claims.Username == "" {
		return "", errors.New("invalid token claims")
	}
	return claims.Username, nil
}

func (s *Service) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		username, err := s.Parse(value)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"missing, invalid or expired JWT token"}`))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, username)))
	})
}

func User(ctx context.Context) string {
	value, _ := ctx.Value(userKey).(string)
	return value
}
