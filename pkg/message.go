package eventcounter

const (
	EventCreated EventType = "created"
	EventUpdated EventType = "updated"
	EventDeleted EventType = "deleted"
)

type EventType string

type MessageBody struct {
	UID string `json:"id"`
}

type Message struct {
	UID       string    `json:"uid"`
	EventType EventType `json:"event_type"`
	UserID    string    `json:"user_id"`
}
