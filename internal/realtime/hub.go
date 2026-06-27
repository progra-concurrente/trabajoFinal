package realtime

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

type client struct {
	connection *websocket.Conn
	writeMu    sync.Mutex
}

func New() *Hub { return &Hub{clients: make(map[*client]struct{})} }

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	connectedClient := &client{connection: connection}
	h.mu.Lock()
	h.clients[connectedClient] = struct{}{}
	h.mu.Unlock()
	connectedClient.writeMu.Lock()
	_ = connection.WriteJSON(map[string]any{"type": "connected", "at": time.Now().UTC()})
	connectedClient.writeMu.Unlock()
	go func() {
		defer func() {
			h.mu.Lock()
			delete(h.clients, connectedClient)
			h.mu.Unlock()
			_ = connection.Close()
		}()
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

func (h *Hub) Broadcast(eventType string, payload any) {
	data, err := json.Marshal(map[string]any{
		"type": eventType, "payload": payload, "at": time.Now().UTC(),
	})
	if err != nil {
		return
	}
	h.mu.RLock()
	clients := make([]*client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	for _, client := range clients {
		client.writeMu.Lock()
		_ = client.connection.SetWriteDeadline(time.Now().Add(2 * time.Second))
		err := client.connection.WriteMessage(websocket.TextMessage, data)
		client.writeMu.Unlock()
		if err != nil {
			h.mu.Lock()
			delete(h.clients, client)
			h.mu.Unlock()
			_ = client.connection.Close()
		}
	}
}
