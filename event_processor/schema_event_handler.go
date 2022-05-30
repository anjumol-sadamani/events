package event_processor

import (
	"encoding/json"
	"errors"
	"eventprocessor/model"
	repo "eventprocessor/repository"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

var ParsedSchemaList []string

type SchemaHandlerService interface {
	SaveSchema(data string) ([]string, error)
}

type SchemaHandlerServiceImpl struct {
	SchemaRepo repo.SchemaRepository
}

func SchemaHandlerServiceCreate(db *gorm.DB) *SchemaHandlerServiceImpl {
	ParsedSchemaList = make([]string, 0)
	return &SchemaHandlerServiceImpl{SchemaRepo: repo.CreateSchemaRepository(db)}
}

func (sh *SchemaHandlerServiceImpl) SaveSchema(inputSchema string) ([]string, error) {
	var schema interface{}
	err := json.Unmarshal([]byte(inputSchema), &schema)
	if err != nil {
		log.Errorf("ParsedSchemaEvent unmarshal error %s", err.Error())
		return nil, errors.New("Invalid JSON")
	}
	sh.parseSchema(schema, "")

	var parsedSchemaEventList []model.ParsedSchemaEvent
	for _, s := range ParsedSchemaList {
		parsedSchema := model.ParsedSchemaEvent{
			SchemaPath: s,
		}
		parsedSchemaEventList = append(parsedSchemaEventList, parsedSchema)
	}
	if err := sh.SchemaRepo.InsertParsedSchema(parsedSchemaEventList); err != nil {
		log.Errorf("Save ParsedSchema error %v", err)
		return nil, err
	}
	log.Info("parsed schema saved successfully")
	return ParsedSchemaList, nil
}

// parse schema recursively and create the schema path list
func (sh *SchemaHandlerServiceImpl) parseSchema(schema interface{}, root string) {
	for key, element := range schema.(map[string]interface{}) {
		newPath := strings.ToLower(key)
		if root != "" {
			newPath = root + "," + newPath
		}
		if reflect.TypeOf(element).Kind() == reflect.Map {
			sh.parseSchema(element, newPath)
		} else {
			ParsedSchemaList = append(ParsedSchemaList, newPath)
		}
	}
}
