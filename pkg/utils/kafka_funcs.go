package utils

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
	"wb-test/internal/handlers"
	"wb-test/internal/models"

	"github.com/go-playground/validator/v10"

	"github.com/segmentio/kafka-go"
)

func RunConsumer(ctx context.Context, wg *sync.WaitGroup, consumer *kafka.Reader, msgChan chan kafka.Message) {
	defer wg.Done()
	defer close(msgChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := consumer.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consumer error: %v", err)
				continue
			}

			select {
			case msgChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func Read_n_process(wg *sync.WaitGroup, service handlers.ServiceInt, mesChan chan kafka.Message, reader *kafka.Reader) {
	defer wg.Done()

	for msg := range mesChan {
		var order models.Order

		err := json.Unmarshal(msg.Value, &order)
		if err != nil {
			reader.CommitMessages(context.Background(), msg)
			log.Println("Kafka parse error")
			continue
		}

		err = data_validate(&order)
		if err != nil {
			reader.CommitMessages(context.Background(), msg)
			log.Println("Message validation error:", err)
			continue
		}

		err = processWithRetry(service, &order)
		if err != nil {
			reader.CommitMessages(context.Background(), msg)
			log.Println("insertion error")
		}
		reader.CommitMessages(context.Background(), msg)
	}
}

func data_validate(order *models.Order) error {
	if order.Delivery != nil {
		if order.Delivery.Phone != "" {
			phone := order.Delivery.Phone
			if phone[0] == '+' {
				for _, char := range phone[1:] {
					if char < '0' || char > '9' {
						return errors.New("phone number can only contain digits after '+' sign")
					}
				}
				if len(phone) < 12 {
					return errors.New("phone number is too short") //должно начинатся с +7 (10) || +8 (10)
				}
			} else {
				for _, char := range phone {
					if char < '0' || char > '9' {
						return errors.New("phone number can only contain digits and optional '+' at the beginning")
					}
				}
				if len(phone) < 11 {
					return errors.New("phone number is too short")
				}
			}
		} else {
			return errors.New("phone number cannot be empty")
		}

		validate := validator.New()
		err := validate.Var(order.Delivery.Email, "email")
		if err != nil {
			return errors.New("email validation error")
		}
	}
	return nil
}

func processWithRetry(service handlers.ServiceInt, order *models.Order) error {
	maxRetries := 3
	retryDelay := 10 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := service.InsertData(ctx, order)
		if err != nil {
			time.Sleep(retryDelay)
			continue
		}
		return nil
	}

	return errors.New("походу все")
}
