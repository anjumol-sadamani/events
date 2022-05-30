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
	CountQueryEventsByMetadata(paths []string, groupBy []string) ([]map[string]interface{}, error)
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
	countQueries := generateCountAggregateQuery(paths)
	var results map[string]interface{}
	println(strings.Join(countQueries, ","))
	err := er.DB.Table("query_events").Select(strings.Join(countQueries, ",")).Find(&results).Error
	if err != nil {
		log.Errorf("Error occurred while fetch the event count from DB")
		return nil, err
	}
	return results, nil
}

func (er *EventRepositoryImpl) GetQueryEventCountByDay(paths []string) ([]map[string]interface{}, error) {
	countQueries := generateCountAggregateQuery(paths)
	var results []map[string]interface{}
	er.DB.Table("query_events").Select("processed_time::date," + strings.Join(countQueries, ",")).Group("processed_time::date").Find(&results)
	return results, nil
}

func (er *EventRepositoryImpl) CountQueryEventsByMetadata(paths []string, groupBy []string) ([]map[string]interface{}, error) {
	countQueries := generateCountAggregateQuery(paths)
	groupQueries := strings.Join(groupBy, ",")
	var results []map[string]interface{}
	er.DB.Table("query_events").Select(groupQueries + "," + strings.Join(countQueries, ",")).Group(groupQueries).Find(&results)
	return results, nil
}

func generateCountAggregateQuery(paths []string) []string {
	countQueries := make([]string, 0, len(paths))
	for _, v := range paths {
		q := strings.Replace(v, "query,", "", 1)
		q = fmt.Sprintf("COUNT(%s #> '{%s}') as \"%s\"", QUERY_TABLE_COLUMN_NAME, q, v)
		countQueries = append(countQueries, q)
	}
	return countQueries
}
