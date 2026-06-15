package transport

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type WSEvent struct {
	Type      string          `json:"type"`
	Timestamp int64           `json:"ts"`
	Payload   json.RawMessage `json:"payload"`
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

type WSHub struct {
	clients    map[*wsClient]bool
	broadcast  chan WSEvent
	register   chan *wsClient
	unregister chan *wsClient
	mu         sync.RWMutex
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*wsClient]bool),
		broadcast:  make(chan WSEvent, 1024),
		register:   make(chan *wsClient, 64),
		unregister: make(chan *wsClient, 64),
	}
}

func (h *WSHub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = true
			h.mu.Unlock()

		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()

		case event := <-h.broadcast:
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.send <- data:
				default:
					// slow client — drop and disconnect
					close(c.send)
					delete(h.clients, c)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *WSHub) Broadcast(eventType string, payload interface{}) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case h.broadcast <- WSEvent{
		Type:      eventType,
		Timestamp: time.Now().UnixMilli(),
		Payload:   json.RawMessage(data),
	}:
	default:
	}
}

func (h *WSHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade error: %v", err)
		return
	}
	c := &wsClient{conn: conn, send: make(chan []byte, 256)}
	h.register <- c
	go c.writePump(h)
	go c.readPump(h)
}

func (c *wsClient) writePump(h *WSHub) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) readPump(h *WSHub) {
	defer func() {
		h.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (h *WSHub) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/ws", h)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	// CORS headers for dashboard
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		mux.ServeHTTP(w, r)
	})
	log.Printf("ws server listening on %s", addr)
	return http.ListenAndServe(addr, handler)
}
