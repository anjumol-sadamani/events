package contoller

import (
	"eventprocessor/model"
	"eventprocessor/service"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type EventRetrieveController struct {
	EventRetrieveService service.EventRetrieveService
}

func (ec *EventRetrieveController) GetEventsCount(c *gin.Context) {
	response := ec.EventRetrieveService.CountEvents()
	c.JSON(response.StatusCode, response.Data)
}

func (ec *EventRetrieveController) GetEventsCountByDay(c *gin.Context) {
	response := ec.EventRetrieveService.CountEventsByDay()
	c.JSON(response.StatusCode, response.Data)
}

func (ec *EventRetrieveController) GetEventsCountByMetadata(c *gin.Context) {
	groupByParams := c.Request.URL.Query().Get("metadata")
	if groupByParams == "" {
		response := model.FailureResponse("metadata params are mandatory", http.StatusBadRequest)
		c.JSON(response.StatusCode, response.Data)
		return
	}
	groupByTags := strings.Split(groupByParams, ",")
	response := ec.EventRetrieveService.CountQueryEventsByMetadata(groupByTags)
	c.JSON(response.StatusCode, response.Data)
}
