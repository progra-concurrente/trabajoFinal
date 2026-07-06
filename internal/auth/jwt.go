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
	secret []byte
	ttl    time.Duration
}

type Claims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type Identity struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}

func New(secret string, ttl time.Duration) *Service {
	return &Service{secret: []byte(secret), ttl: ttl}
}

func (s *Service) Issue(username, role string) (string, error) {
	if username == "" {
		return "", errors.New("username is required")
	}
	if role == "" {
		role = "user"
	}
	now := time.Now()
	claims := Claims{
		Username: username, Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: username, IssuedAt: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

func (s *Service) Parse(tokenValue string) (Identity, error) {
	token, err := jwt.ParseWithClaims(tokenValue, &Claims{}, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, errors.New("unexpected signing method")
		}
		return s.secret, nil
	})
	if err != nil || !token.Valid {
		return Identity{}, errors.New("invalid or expired token")
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || claims.Username == "" {
		return Identity{}, errors.New("invalid token claims")
	}
	role := claims.Role
	if role == "" {
		role = "user"
	}
	return Identity{Username: claims.Username, Role: role}, nil
}

func (s *Service) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		identity, err := s.Parse(value)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"missing, invalid or expired JWT token"}`))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, identity)))
	})
}

func User(ctx context.Context) string {
	return IdentityFromContext(ctx).Username
}

func Role(ctx context.Context) string {
	return IdentityFromContext(ctx).Role
}

func IsAdmin(ctx context.Context) bool {
	return Role(ctx) == "admin"
}

func IdentityFromContext(ctx context.Context) Identity {
	value, _ := ctx.Value(userKey).(Identity)
	return value
}
