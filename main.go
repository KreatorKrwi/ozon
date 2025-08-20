package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"wb-test/configs"
	"wb-test/internal/cache"
	"wb-test/internal/handlers"
	"wb-test/internal/repository"
	"wb-test/internal/service"
	"wb-test/pkg/utils"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

//go:embed frontend/*
var frontendFS embed.FS

func setupRouter(handler *handlers.Handler, frontendFS embed.FS) *gin.Engine {
	r := gin.Default()

	frontendDir, _ := fs.Sub(frontendFS, "frontend")
	r.StaticFS("/static", http.FS(frontendDir))

	r.GET("/", func(c *gin.Context) {
		data, _ := fs.ReadFile(frontendDir, "index.html")
		c.Data(http.StatusOK, "text/html", data)
	})

	r.GET("/order/:order_id", handler.GetById)

	return r
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	//подгружаем конфиг
	cfg, err := configs.Load()
	if err != nil {
		log.Fatal("Config load error")
	}

	//пробрасываем конект к бд
	db, err := utils.Db_conn(cfg)
	if err != nil {
		log.Fatal("Database connection error")
	}
	defer db.Close()

	//выплевываем редис
	rediska := utils.Redis_conn()
	if rediska == nil {
		log.Fatal("Redis conn error")
	}
	defer rediska.Close()

	//подключаем кафку
	reader := utils.Kafka_consumer_conn()
	if reader == nil {
		log.Fatal("Kafka reader error")
	}
	defer reader.Close()

	//инициализируем слои
	repository := repository.NewRepo(db)
	cache := cache.NewCache(rediska)
	service := service.NewService(repository, cache)
	handler := handlers.NewHandlers(service)

	err = repository.CreateTablesIfNotExist()
	if err != nil {
		log.Fatal("Tables creation error")
	}

	//подгрузка в кэш
	go func() {
		var ers []error = service.GetInitCache(20)
		for _, er := range ers {
			log.Println(er)
		}
	}()

	//запуск консюмера + логики
	mesChan := make(chan kafka.Message, 100)
	var wg sync.WaitGroup

	wg.Add(1)
	go utils.RunConsumer(ctx, &wg, reader, mesChan)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go utils.Read_n_process(&wg, service, mesChan, reader)
	}

	//запуска сервера
	server := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: setupRouter(handler, frontendFS),
	}

	go func() {
		log.Printf("Server starting on port %s", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	//graceful shutdown
	<-ctx.Done()
	log.Println("Received shutdown signal")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	wg.Wait()
	log.Println("All goroutines finished")
}
