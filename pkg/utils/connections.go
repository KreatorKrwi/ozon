package utils

import (
	"wb-test/configs"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func Db_conn(cfg *configs.Config) (*sqlx.DB, error) {

	connSrt :=
		"host=" + cfg.DB.Host +
			" port=" + cfg.DB.Port +
			" user=" + cfg.DB.User +
			" password=" + cfg.DB.Password +
			" dbname=" + cfg.DB.Name +
			" sslmode=" + cfg.DB.SSLMode

	db, err := sqlx.Connect("postgres", connSrt)
	return db, err
}

func Redis_conn() *redis.Client {
	rediska := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "ozon_top",
		DB:       0,
	})

	return rediska
}

func Kafka_consumer_conn() *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"kafka:9092"},
		Topic:          "orders",
		GroupID:        "orders-group",
		GroupBalancers: []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
		StartOffset:    kafka.FirstOffset,
	})

	return reader
}
