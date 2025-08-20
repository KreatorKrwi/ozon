package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"wb-test/internal/models"
)

type RepoInt interface {
	CreateTablesIfNotExist() error
	Get(ctx context.Context, orderUID string) (*models.Order, error)
	Insert(ctx context.Context, order *models.Order) error
	GetIDS(ctx context.Context, n int) ([]string, error)
}

type CashInt interface {
	GetData(orderUID string) (string, error)
	SetData(json []byte, orderUID string) error
}

type Service struct {
	repo RepoInt
	cash CashInt
}

func NewService(repo RepoInt, cash CashInt) *Service {
	return &Service{repo: repo, cash: cash}
}

func (s *Service) InsertData(ctx context.Context, order *models.Order) error {

	err := s.repo.Insert(ctx, order)
	if err != nil {
		return err
	}

	go func(id string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		data, err := s.repo.Get(ctx, id)
		if err != nil {
			log.Println("Db get error:", err)
			return
		}
		s.SetToCache(data)
	}(order.OrderUID)

	return nil
}

func (s *Service) PullData(ctx context.Context, orderUID string) (*models.Order, error) {

	data, err := s.repo.Get(ctx, orderUID)
	if err != nil {
		return nil, err
	}
	return data, err
}

func (s *Service) SetToCache(order *models.Order) {
	jsonData, err := json.Marshal(order)
	if err != nil {
		log.Println("Cache insert error:", err)
		return
	}
	err = s.cash.SetData(jsonData, order.OrderUID)
	if err != nil {
		log.Println("Cache insert error:", err)
		return
	}
}

func (s *Service) GetFromCache(id string) (*models.Order, error) {

	cachedOrder := &models.Order{}

	data, err := s.cash.GetData(id)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(data), cachedOrder); err != nil {
		log.Println("Get cache unmarshal error:", err)
		return nil, err
	}
	return cachedOrder, nil
}

func (s *Service) GetInitCache(n int) []error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ids, err := s.repo.GetIDS(ctx, n)

	if errors.Is(err, context.DeadlineExceeded) {
		return []error{fmt.Errorf("db query timeout: %w", err)}
	} else if err != nil {
		return []error{fmt.Errorf("failed to get orders: %w", err)}
	}

	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, 20)
		errChan   = make(chan error, len(ids))
	)

	for _, uid := range ids {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			cacheCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			if err := s.GetSingleOrder(cacheCtx, uid); err != nil {
				errChan <- fmt.Errorf("order %s: %w", uid, err)
			}
		}(uid)
	}

	go func() { wg.Wait(); close(errChan) }()

	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	return errors
}

func (s *Service) GetSingleOrder(ctx context.Context, uid string) error {

	order, err := s.repo.Get(ctx, uid)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	err = s.cash.SetData(jsonData, uid)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}

	return nil
}
