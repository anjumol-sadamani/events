package contoller

import (
	"encoding/json"
	"eventprocessor/model"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_GetEventsCount(t *testing.T) {

	type testData struct {
		name        string
		apiResponse *model.APIResponse
		httpStatus  int
		mockClosure func(mock *MockService)
	}
	failureResponse := model.FailureResponse("Query not found", http.StatusNotFound)
	successResponse := model.SuccessResponse(`{
		"deal": {
			"price": 5,
			"title": 7,
			"user": 7
		},
		"query": {
			"deal": 0,
			"user": 0
		},
		"user": {
			"deals": 0,
			"name": 2
		}
	}`)
	tests := []testData{
		{
			name:        "Success",
			apiResponse: successResponse,
			httpStatus:  successResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountEvents").Return(successResponse)
			},
		},
		{
			name:        "Query not found",
			apiResponse: failureResponse,
			httpStatus:  failureResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountEvents").Return(failureResponse)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := httptest.NewRecorder()
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(actual)
			mockService := &MockService{}
			test.mockClosure(mockService)

			ec := &EventRetrieveController{
				EventRetrieveService: mockService,
			}
			ec.GetEventsCount(c)

			assert.Equal(t, test.httpStatus, actual.Code)
			if actual.Code != http.StatusOK {

				response := model.ErrorMessage{}
				err := json.Unmarshal([]byte(actual.Body.String()), &response)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, &response)

			} else {

				var data string
				err := json.Unmarshal([]byte(actual.Body.String()), &data)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, data)

			}
		})
	}
}

func Test_GetEventsCountByDay(t *testing.T) {

	type testData struct {
		name        string
		apiResponse *model.APIResponse
		httpStatus  int
		mockClosure func(mock *MockService)
	}
	failureResponse := model.FailureResponse("Query not found", http.StatusNotFound)
	successResponse := model.SuccessResponse(`[
		{
			"deal": {
				"price": 192,
				"title": 215,
				"user": 215
			},
			"processed_time": "2022-05-09T00:00:00Z",
			"query": {
				"deal": 0,
				"user": 0
			},
			"user": {
				"deals": 0,
				"name": 95
			}
		}
	]`)
	tests := []testData{
		{
			name:        "Success",
			apiResponse: successResponse,
			httpStatus:  successResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountEventsByDay").Return(successResponse)
			},
		},
		{
			name:        "Query not found",
			apiResponse: failureResponse,
			httpStatus:  failureResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountEventsByDay").Return(failureResponse)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := httptest.NewRecorder()
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(actual)
			mockService := &MockService{}
			test.mockClosure(mockService)

			ec := &EventRetrieveController{
				EventRetrieveService: mockService,
			}
			ec.GetEventsCountByDay(c)

			assert.Equal(t, test.httpStatus, actual.Code)
			if actual.Code != http.StatusOK {

				response := model.ErrorMessage{}
				err := json.Unmarshal([]byte(actual.Body.String()), &response)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, &response)

			} else {

				var data string
				err := json.Unmarshal([]byte(actual.Body.String()), &data)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, data)

			}
		})
	}
}

func Test_GetEventsCountByMetadata(t *testing.T) {

	type testData struct {
		name        string
		apiResponse *model.APIResponse
		httpStatus  int
		Query       string
		mockClosure func(mock *MockService)
	}

	metadata := []string{"Client"}
	failureResponse := model.FailureResponse("Query not found", http.StatusNotFound)
	badRequestResponse := model.FailureResponse("metadata params are mandatory", http.StatusBadRequest)
	successResponse := model.SuccessResponse(`[
		{
			"deal": {
				"price": 192,
				"title": 215,
				"user": 215
			},
			"processed_time": "2022-05-09T00:00:00Z",
			"query": {
				"deal": 0,
				"user": 0
			},
			"user": {
				"deals": 0,
				"name": 95
			}
		}
	]`)

	tests := []testData{
		{
			name:        "Success",
			apiResponse: successResponse,
			httpStatus:  successResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountQueryEventsByMetadata", metadata).Return(successResponse)
			},
			Query: metadata[0],
		},
		{
			name:        "Query not found",
			apiResponse: failureResponse,
			httpStatus:  failureResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountQueryEventsByMetadata", metadata).Return(failureResponse)
			},
			Query: metadata[0],
		},
		{
			name:        "Mandatory params",
			apiResponse: badRequestResponse,
			httpStatus:  badRequestResponse.StatusCode,
			mockClosure: func(mock *MockService) {
				mock.On("CountQueryEventsByMetadata", metadata).Return(failureResponse)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/event-processor/api/v1/countByMetadata", nil)
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(actual)
			q := req.URL.Query()
			q.Add("metadata", test.Query)
			req.URL.RawQuery = q.Encode()
			c.Request = req
			mockService := &MockService{}
			test.mockClosure(mockService)

			ec := &EventRetrieveController{
				EventRetrieveService: mockService,
			}
			ec.GetEventsCountByMetadata(c)

			assert.Equal(t, test.httpStatus, actual.Code)
			if actual.Code != http.StatusOK {
				response := model.ErrorMessage{}
				err := json.Unmarshal([]byte(actual.Body.String()), &response)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, &response)

			} else {

				var data string
				err := json.Unmarshal([]byte(actual.Body.String()), &data)
				if err != nil {
					fmt.Println("err", err.Error())
				}
				assert.Equal(t, test.apiResponse.Data, data)

			}
		})
	}
}

type MockService struct {
	mock.Mock
}

func (m *MockService) CountEvents() *model.APIResponse {
	args := m.Called()
	return args.Get(0).(*model.APIResponse)
}

func (m *MockService) CountEventsByDay() *model.APIResponse {
	args := m.Called()
	return args.Get(0).(*model.APIResponse)
}

func (m *MockService) CountQueryEventsByMetadata(metadata []string) *model.APIResponse {
	args := m.Called(metadata)
	return args.Get(0).(*model.APIResponse)
}
