package model

import "time"

type ParsedSchemaEvent struct {
	Id         int64
	SchemaPath string
}

type QueryEvent struct {
	Client        string
	ClientVersion string
	DataCenter    string
	ProcessedTime time.Time
	Query         string
}

//event format from message queue
//todo add metadata of client
type EventInfo struct {
	EventType             string
	Data                  string
	RetryCount            int
	ProcessAfterTimeStamp time.Time
}
