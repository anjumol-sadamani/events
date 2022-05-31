package repository

import (
	"eventprocessor/model"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	QUERY_TABLE_COLUMN_NAME = "query"
)

type EventRepository interface {
	InsertQueryEvent(queryEvent model.QueryEvent) error
	GetQueryEventCount(paths []string) (map[string]interface{}, error)
	GetQueryEventCountByDay(paths []string) ([]map[string]interface{}, error)
	CountQueryEventsByMetadata(paths []string, metaData []string) ([]map[string]interface{}, error)
}

type EventRepositoryImpl struct {
	DB *gorm.DB
}

func CreateEventRepository(db *gorm.DB) *EventRepositoryImpl {
	return &EventRepositoryImpl{DB: db}
}

func (er *EventRepositoryImpl) InsertQueryEvent(md model.QueryEvent) error {
	err := er.DB.Table("query_events").Create(&md).Error
	return err

}

func (er *EventRepositoryImpl) GetQueryEventCount(paths []string) (map[string]interface{}, error) {
	countQueries := generateCountQuery(paths)
	var results map[string]interface{}
	err := er.DB.Table("query_events").Select(strings.Join(countQueries, ",")).Find(&results).Error
	if err != nil {
		log.Errorf("Error occurred while fetch the event count from DB")
		return nil, err
	}
	return results, nil
}

func (er *EventRepositoryImpl) GetQueryEventCountByDay(paths []string) ([]map[string]interface{}, error) {
	countQueries := generateCountQuery(paths)
	var results []map[string]interface{}
	er.DB.Table("query_events").Select("processed_time::date," + strings.Join(countQueries, ",")).Group("processed_time::date").Find(&results)
	return results, nil
}

func (er *EventRepositoryImpl) CountQueryEventsByMetadata(paths []string, metaData []string) ([]map[string]interface{}, error) {
	countQueries := generateCountQuery(paths)
	metaDatas := strings.Join(metaData, ",")
	var results []map[string]interface{}
	er.DB.Table("query_events").Select(metaDatas + "," + strings.Join(countQueries, ",")).Group(metaDatas).Find(&results)
	return results, nil
}

func generateCountQuery(paths []string) []string {
	countQueries := make([]string, 0, len(paths))
	for _, pathFromSchema := range paths {
		pathInQuery := strings.Replace(pathFromSchema, "query,", "", 1)
		pathInQuery = fmt.Sprintf("COUNT(%s #> '{%s}') as \"%s\"", QUERY_TABLE_COLUMN_NAME, pathInQuery, pathFromSchema)
		countQueries = append(countQueries, pathInQuery)
	}
	return countQueries
}
