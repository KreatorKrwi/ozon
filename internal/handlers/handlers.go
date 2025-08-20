package handlers

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"wb-test/internal/models"

	"github.com/gin-gonic/gin"
)

type ServiceInt interface {
	InsertData(ctx context.Context, order *models.Order) error
	PullData(ctx context.Context, orderUID string) (*models.Order, error)
	SetToCache(order *models.Order)
	GetFromCache(id string) (*models.Order, error)
}

type Handler struct {
	service ServiceInt
}

func NewHandlers(service ServiceInt) *Handler {
	return &Handler{service: service}
}

func (h *Handler) GetById(c *gin.Context) {

	orderID := c.Param("order_id")

	cachedOrder, err := h.service.GetFromCache(orderID)
	if err == nil {
		log.Println("Нашли в редисе")
		c.JSON(http.StatusOK, cachedOrder)
		return
	} else {
		data, err := h.service.PullData(c.Request.Context(), orderID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
				return
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}
		c.JSON(http.StatusOK, data)

		go h.service.SetToCache(data)
	}
}
