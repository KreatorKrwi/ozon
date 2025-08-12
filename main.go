package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

//go:embed frontend/*
var frontendFS embed.FS

func runConsumer(consumer *kafka.Reader, msgChan chan<- kafka.Message) {

	for {
		msg, err := consumer.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue
		}
		msgChan <- msg
	}
}

func main() {

	// пробрасываем конект к бд
	cfg, err := Load()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}
	connSrt :=
		"host=" + cfg.DB.Host +
			" port=" + cfg.DB.Port +
			" user=" + cfg.DB.User +
			" password=" + cfg.DB.Password +
			" dbname=" + cfg.DB.Name +
			" sslmode=" + cfg.DB.SSLMode

	db, err := sqlx.Connect("postgres", connSrt)
	if err != nil {
		log.Fatal("Failed to connect to DB:", err)
	}
	defer db.Close()

	//выплевываем редис
	rediska := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "ozon_top",
		DB:       0,
	})

	repository := NewRepo(db, rediska)

	repository.CreateTablesIfNotExist()

	go func() {
		var ers []error = repository.GetCache(100)
		for _, er := range ers {
			log.Println(er)
		}
	}()

	//подключаем кафку
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"kafka:9092"},
		Topic:          "orders",
		GroupID:        "orders-group",
		GroupBalancers: []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
		StartOffset:    kafka.FirstOffset,
	})

	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer reader.Close()

	mesChan := make(chan kafka.Message, 100)
	go runConsumer(reader, mesChan)
	defer close(mesChan)

	for i := 0; i < 3; i++ {
		go func() {
			for msg := range mesChan {
				var order Order
				err = json.Unmarshal(msg.Value, &order)
				if err != nil {
					reader.CommitMessages(context.Background(), msg) //почему вообще в сервис из другого сервиса должны приходить плохие данные? Пусть там и проверяют заранее
					log.Println("Ошибка парсинга из кафки")
				}
				err = repository.Insert(context.Background(), &order) //ретраев не будет, тогда и dlq придется делать, я устал
				if err != nil {
					log.Println("Ошибка вставки")
				}
				reader.CommitMessages(context.Background(), msg)
			}
		}()
	}

	r := gin.Default()

	frontendDir, _ := fs.Sub(frontendFS, "frontend")
	r.StaticFS("/static", http.FS(frontendDir))

	r.GET("/", func(c *gin.Context) {
		data, _ := fs.ReadFile(frontendDir, "index.html")
		c.Data(http.StatusOK, "text/html", data)
	})

	r.GET("/order/:order_id", func(ctx *gin.Context) {
		orderID := ctx.Param("order_id")

		redisCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		val, err := rediska.Get(redisCtx, orderID).Result()
		if err == nil {
			var cachedOrder Order
			if err := json.Unmarshal([]byte(val), &cachedOrder); err == nil {
				log.Println("Нашли в редисе")
				ctx.JSON(http.StatusOK, cachedOrder)
				return
			}
		} else {

			data, err := repository.Get(context.Background(), orderID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					ctx.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
				} else {
					ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				}
				return
			}
			redisCtx1, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if jsonData, err := json.Marshal(data); err == nil {
				rediska.Set(redisCtx1, orderID, jsonData, time.Hour)
			}

			ctx.JSON(http.StatusOK, data)
		}
	})

	r.Run(":" + cfg.Server.Port)
}
