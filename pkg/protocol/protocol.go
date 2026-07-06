package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
)

const (
	Register       = "register"
	Heartbeat      = "heartbeat"
	Predict        = "predict"
	TrainInit      = "train_init"
	TrainEpoch     = "train_epoch"
	GradientResult = "gradient_result"
	Error          = "error"
)

type Message struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	NodeID  string          `json:"node_id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type Registration struct {
	NodeID       string `json:"node_id"`
	Capacity     int    `json:"capacity"`
	ModelVersion string `json:"model_version"`
}

type TrainInitPayload struct {
	DatasetPath string `json:"dataset_path,omitempty"`
}

type TrainInitResult struct {
	Rows int `json:"rows"`
}

type HeartbeatStatus struct {
	ReceivedAt       int64   `json:"received_at"`
	ActiveJobs       int     `json:"active_jobs"`
	Capacity         int     `json:"capacity"`
	CPUUsagePercent  float64 `json:"cpu_usage_percent"`
	LoadedRecordRows int     `json:"loaded_record_rows"`
}

type TrainEpochPayload struct {
	Start   int       `json:"start"`
	End     int       `json:"end"`
	Weights []float64 `json:"weights"`
}

func NewMessage(id, messageType string, payload any) (Message, error) {
	raw, err := json.Marshal(payload)
	return Message{ID: id, Type: messageType, Payload: raw}, err
}

func Decode(reader *bufio.Reader) (Message, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return Message{}, errors.New("protocol message is not newline terminated")
		}
		return Message{}, err
	}
	var message Message
	if err := json.Unmarshal(line, &message); err != nil {
		return Message{}, err
	}
	if message.ID == "" || message.Type == "" {
		return Message{}, errors.New("protocol message requires id and type")
	}
	return message, nil
}

func Encode(writer *bufio.Writer, message Message) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if _, err := writer.Write(append(data, '\n')); err != nil {
		return err
	}
	return writer.Flush()
}

func UnmarshalPayload(message Message, target any) error {
	if len(message.Payload) == 0 {
		return errors.New("message payload is empty")
	}
	return json.Unmarshal(message.Payload, target)
}
