package event_processor

import (
	"encoding/json"
	"errors"
	"eventprocessor/model"
	repo "eventprocessor/repository"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type QueryEventHandlerService interface {
	SaveQueryEvent(data string) error
}

type QueryEventHandlerServiceImpl struct {
	EventRepo repo.EventRepository
}

func EventHandlerServiceCreate(db *gorm.DB) *QueryEventHandlerServiceImpl {
	return &QueryEventHandlerServiceImpl{EventRepo: repo.CreateEventRepository(db)}
}

func (eh *QueryEventHandlerServiceImpl) SaveQueryEvent(query string) error {
	if !json.Valid([]byte(query)) {
		log.Errorf("Save queryEvent error - Invalid JSON : %s", query)
		return errors.New("Invalid JSON")
	}

	queryEvent := model.QueryEvent{
		Client:        "client_id",
		ClientVersion: "v1",
		DataCenter:    "Google",
		ProcessedTime: time.Now(),
		Query:         query,
	}
	if err := eh.EventRepo.InsertQueryEvent(queryEvent); err != nil {
		log.Errorf("Save queryEvent error %v", err)
		return errors.New("DB Save failed")
	}
	log.Info("queryEvent saved successfully")
	return nil
}
