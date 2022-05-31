package event_processor

import (
	"context"
	"errors"
	"eventprocessor/config"
	"eventprocessor/kafka"
	"eventprocessor/model"
	"os"
	"sync"

	kf "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	SCHEMA_TYPE     = "Schema"
	QUERY_TYPE      = "Query"
	MAX_RETRY_COUNT = 10
)

type EventListener struct {
	Ctx           context.Context
	AppConfig     *config.Config
	KafkaConfig   *kafka.KafkaConfig
	EventService  QueryEventHandlerService
	SchemaService SchemaHandlerService
	EventChannel  chan model.EventInfo
	Wg            *sync.WaitGroup
	StopChannel   chan os.Signal
}

func (el *EventListener) ReadEvents() {
	defer el.Wg.Done()
	for {
		el.readEventsFromKafka()
	}
}

func (el *EventListener) readEventsFromKafka() {
	message, err := el.KafkaConfig.Reader.Read(el.Ctx)
	if err != nil {
		log.Errorf("Events has failed to get consumed: %v", err.Error())
		return
	}

	//committing the message on the kafka topic.
	defer func() {
		if err := el.KafkaConfig.Reader.CommitMessage(el.Ctx, *message); err != nil {
			log.Error(err)
		}
	}()

	//event is successfully retrieved.
	log.Infof("message at event id/topic/partition/offset/time %s/%v/%v/%v/%s: %s\n", string(message.Key), message.Topic, message.Partition, message.Offset, message.Time.String(), string(message.Key))

	data := string(message.Value)
	eventType, err := getEventHeader(message.Headers)

	if err != nil {
		log.Errorf("Event type error %s", err.Error())
		return
	}

	eventInfo := model.EventInfo{
		Data:      data,
		EventType: eventType,
	}

	switch eventInfo.EventType {
	case SCHEMA_TYPE:
		_, err := el.SchemaService.SaveSchema(data)
		if err != nil {
			log.Error(err)
		}
	case QUERY_TYPE:
		el.EventChannel <- eventInfo
	default:
		log.Warnf("invalid QueryEvent type received %s", eventInfo.EventType)
	}

}

func (el *EventListener) PersistQuery() {
	defer el.Wg.Done()
	for {
		if el.persistQueryFromKafka() {
			return
		}
	}
}

func (el *EventListener) persistQueryFromKafka() bool {
	select {
	case eventInfo := <-el.EventChannel:
		if dbError := el.EventService.SaveQueryEvent(eventInfo.Data); dbError != nil {
			el.handleRetry(eventInfo)
		}
	case sig := <-el.StopChannel:
		log.Errorf("Got %s signal. Aborting ..!", sig)
		return true
	}
	return false
}

func (el *EventListener) handleRetry(ei model.EventInfo) {
	if ei.RetryCount < MAX_RETRY_COUNT {
		err := el.KafkaConfig.RetryEvent(el.Ctx, &ei)
		if err != nil {
			log.Error(err)
		}
	} else {
		// Events are pushed in the DLQ after all the retries
		err := el.KafkaConfig.WriteMessageToDLQTopic(el.Ctx, &ei)
		if err != nil {
			log.Error(err)

		}
	}
}

func getEventHeader(headers []kf.Header) (string, error) {
	for _, header := range headers {
		if header.Key == "type" {
			return string(header.Value), nil
		}
	}
	return "", errors.New("event type not found in the event")
}
