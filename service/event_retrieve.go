package service

import (
	"eventprocessor/event_processor"
	"eventprocessor/model"
	repo "eventprocessor/repository"
	"github.com/Jeffail/gabs"
	"gorm.io/gorm"
	"net/http"
	"strings"
)

type EventRetrieveService interface {
	CountEvents() *model.APIResponse
	CountEventsByDay() *model.APIResponse
	CountQueryEventsByMetadata(groupBy []string) *model.APIResponse
}

type EventRetrieveServiceImpl struct {
	EventRepo repo.EventRepository
}

func NewServiceCreate(db *gorm.DB) *EventRetrieveServiceImpl {
	return &EventRetrieveServiceImpl{EventRepo: repo.CreateEventRepository(db)}
}

func (e *EventRetrieveServiceImpl) CountEvents() *model.APIResponse {
	res, err := e.EventRepo.GetQueryEventCount(event_processor.ParsedSchemaList)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}

	if len(res) <= 0 {
		return model.FailureResponse("Query not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONObject(res))
}

func (e *EventRetrieveServiceImpl) CountEventsByDay() *model.APIResponse {
	res, err := e.EventRepo.GetQueryEventCountByDay(event_processor.ParsedSchemaList)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}
	if len(res) <= 0 {
		return model.FailureResponse("Query not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONArray(res))
}

func (e *EventRetrieveServiceImpl) CountQueryEventsByMetadata(metaData []string) *model.APIResponse {
	res, err := e.EventRepo.CountQueryEventsByMetadata(event_processor.ParsedSchemaList, metaData)

	if err != nil {
		return model.FailureResponse("Failed to get count", http.StatusInternalServerError)
	}
	if len(res) <= 0 {
		return model.FailureResponse("Query not available", http.StatusNotFound)
	}

	return model.SuccessResponse(generateJSONArray(res))
}

func generateJSONArray(dataMapArray []map[string]interface{}) interface{} {
	jsonArray, _ := gabs.New().Array()
	for i := 0; i < len(dataMapArray); i++ {
		jsonArray.ArrayAppend(generateJSONObject(dataMapArray[i]))
	}

	return jsonArray.Data()
}

func generateJSONObject(dataMap map[string]interface{}) interface{} {
	jsonObj := gabs.New()
	for key, value := range dataMap {
		key = strings.ReplaceAll(key, ",", ".")
		jsonObj.SetP(value, key)
	}

	return jsonObj.Data()
}
