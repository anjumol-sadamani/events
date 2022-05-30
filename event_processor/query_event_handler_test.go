package event_processor

import (
	"errors"
	"eventprocessor/model"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_SaveEvent(t *testing.T) {

	type testData struct {
		name        string
		data        string
		result      interface{}
		mockClosure func(mock *MockRepo)
	}

	mockTime := time.Now()

	tests := []testData{
		{
			name: "Success",
			data: `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
			mockClosure: func(mock *MockRepo) {
				mock.On("InsertQueryEvent",
					model.QueryEvent{
						Client:        "client_id",
						ClientVersion: "v1",
						DataCenter:    "Google",
						ProcessedTime: mockTime,
						Query:         `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
					}).Return(nil).Once()
			},
			result: nil,
		},
		{
			name: "DB-Fail",
			data: `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
			mockClosure: func(mock *MockRepo) {
				mock.On("InsertQueryEvent",
					model.QueryEvent{
						Client:        "client_id",
						ClientVersion: "v1",
						DataCenter:    "Google",
						ProcessedTime: mockTime,
						Query:         `{"deal":{"price":true},"user":{"id":true,"name":true}}`,
					}).Return(errors.New("")).Once()
			},
			result: errors.New("DB Save failed"),
		},
		{
			name: "InValidJSON",
			data: ``,
			mockClosure: func(mockRepo *MockRepo) {
				mockRepo.On("InsertQueryEvent", mock.Anything).Return(nil).Times(0)
			},
			result: errors.New("Invalid JSON"),
		},
	}

	patchTime := monkey.Patch(time.Now, func() time.Time { return mockTime })
	defer patchTime.Unpatch()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			mockRepo := &MockRepo{}
			test.mockClosure(mockRepo)

			ec := &QueryEventHandlerServiceImpl{
				EventRepo: mockRepo,
			}

			er := ec.SaveQueryEvent(test.data)

			assert.Equal(t, test.result, er)
		})
	}
}

type MockRepo struct {
	mock.Mock
}

func (m *MockRepo) InsertQueryEvent(data model.QueryEvent) error {
	args := m.Called(data)
	return args.Error(0)
}

func (m *MockRepo) GetQueryEventCount(paths []string) (map[string]interface{}, error) {
	args := m.Called(paths)
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func (m *MockRepo) GetQueryEventCountByDay(paths []string) ([]map[string]interface{}, error) {
	args := m.Called(paths)
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockRepo) CountQueryEventsByMetadata(paths []string, groupBy []string) ([]map[string]interface{}, error) {
	args := m.Called(paths, groupBy)
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}
