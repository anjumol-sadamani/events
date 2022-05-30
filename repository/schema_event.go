package repository

import (
	"eventprocessor/model"
	"gorm.io/gorm"
)

type SchemaRepository interface {
	InsertParsedSchema(schemaColumn []model.ParsedSchemaEvent) error
}

type SchemaRepositoryImpl struct {
	DB *gorm.DB
}

func CreateSchemaRepository(db *gorm.DB) *SchemaRepositoryImpl {
	return &SchemaRepositoryImpl{DB: db}
}

func (sr *SchemaRepositoryImpl) InsertParsedSchema(parsedSchema []model.ParsedSchemaEvent) error {
	return sr.DB.Table("parsed_schema_events").Create(&parsedSchema).Error
}
