package protocol

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestProtocolHandlesPartialTCPWrites(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() {
		parts := []string{
			`{"id":"abc",`,
			`"type":"heartbeat",`,
			`"payload":{"ok":true}}`,
			"\n",
		}
		for _, part := range parts {
			_, _ = client.Write([]byte(part))
			time.Sleep(time.Millisecond)
		}
	}()
	message, err := Decode(bufio.NewReader(server))
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != "abc" || message.Type != Heartbeat {
		t.Fatalf("unexpected message: %+v", message)
	}
}

func TestProtocolRejectsMalformedAndIncompleteMessages(t *testing.T) {
	for _, input := range []string{`{"id":"x"}` + "\n", `not-json` + "\n", `{"id":"x","type":"heartbeat"}`} {
		server, client := net.Pipe()
		go func(input string) {
			_, _ = client.Write([]byte(input))
			_ = client.Close()
		}(input)
		_, err := Decode(bufio.NewReader(server))
		_ = server.Close()
		if err == nil {
			t.Fatalf("expected %q to fail", input)
		}
	}
}

func TestEncodeProducesNewlineDelimitedJSON(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		done <- Encode(bufio.NewWriter(client), Message{
			ID: "1", Type: Heartbeat, Payload: json.RawMessage(`{"ok":true}`),
		})
	}()
	line, err := bufio.NewReader(server).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line[len(line)-1] != '\n' {
		t.Fatal("message is not newline terminated")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
