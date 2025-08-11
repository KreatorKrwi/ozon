package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Repo struct {
	db    *sqlx.DB
	redis *redis.Client
}

func (r *Repo) CreateTablesIfNotExist() error {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS orders (
			order_uid VARCHAR(255) PRIMARY KEY,
			track_number VARCHAR(255),
			entry VARCHAR(255),
			locale VARCHAR(10),
			internal_signature VARCHAR(255),
			customer_id VARCHAR(255),
			delivery_service VARCHAR(255),
			shardkey VARCHAR(255),
			sm_id INTEGER,
			date_created VARCHAR(255),
			oof_shard VARCHAR(255)
		)`,
		`CREATE TABLE IF NOT EXISTS deliveries (
			order_uid VARCHAR(255) PRIMARY KEY REFERENCES orders(order_uid),
			name VARCHAR(255),
			phone VARCHAR(255),
			zip VARCHAR(255),
			city VARCHAR(255),
			address VARCHAR(255),
			region VARCHAR(255),
			email VARCHAR(255)
		)`,
		`CREATE TABLE IF NOT EXISTS payments (
			order_uid VARCHAR(255) PRIMARY KEY REFERENCES orders(order_uid),
			transaction VARCHAR(255),
			request_id VARCHAR(255),
			currency VARCHAR(10),
			provider VARCHAR(255),
			amount INTEGER,
			payment_dt BIGINT,
			bank VARCHAR(255),
			delivery_cost INTEGER,
			goods_total INTEGER,
			custom_fee INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS items (
			id SERIAL PRIMARY KEY,
			order_uid VARCHAR(255) REFERENCES orders(order_uid),
			chrt_id INTEGER,
			track_number VARCHAR(255),
			price INTEGER,
			rid VARCHAR(255),
			name VARCHAR(255),
			sale INTEGER,
			size VARCHAR(255),
			total_price INTEGER,
			nm_id INTEGER,
			brand VARCHAR(255),
			status INTEGER
		)`,
	}

	for _, table := range tables {
		_, err := r.db.Exec(table)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

func NewRepo(db *sqlx.DB, redis *redis.Client) *Repo {
	return &Repo{db: db, redis: redis}
}

func (r *Repo) GetCache(n int) []error {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var orderUIDs []string
	if err := r.db.SelectContext(ctx, &orderUIDs, `
		SELECT order_uid FROM orders 
		ORDER BY to_timestamp(date_created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') DESC 
		LIMIT $1`, n); err != nil {

		if errors.Is(err, context.DeadlineExceeded) {
			return []error{fmt.Errorf("db query timeout: %w", err)}
		}
		return []error{fmt.Errorf("failed to get orders: %w", err)}
	}
	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, 20)
		errChan   = make(chan error, len(orderUIDs))
	)

	for _, uid := range orderUIDs {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := r.cacheOrder(cacheCtx, uid); err != nil {
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

func (r *Repo) cacheOrder(ctx context.Context, uid string) error {

	order, err := r.Get(ctx, uid)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := r.redis.Set(ctx, uid, jsonData, 5*time.Minute).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	return nil
}

func (r *Repo) Get(ctx context.Context, orderUID string) (*Order, error) {
	order := &Order{}

	err := r.db.GetContext(ctx, order, `
        SELECT * FROM orders WHERE order_uid = $1`, orderUID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("order not found")
		}
		return nil, fmt.Errorf("order select: %w", err)
	}

	var delivery Delivery
	err = r.db.GetContext(ctx, &delivery, `
        SELECT name, phone, zip, city, address, region, email 
        FROM deliveries WHERE order_uid = $1`, orderUID)
	if err == nil {
		order.Delivery = &delivery
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("delivery select: %w", err)
	}

	var payment Payment
	err = r.db.GetContext(ctx, &payment, `
        SELECT transaction, currency, provider, amount, payment_dt,
               bank, delivery_cost, goods_total, custom_fee
        FROM payments WHERE order_uid = $1`, orderUID)
	if err == nil {
		order.Payment = &payment
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("payment select: %w", err)
	}

	var items []Item
	err = r.db.SelectContext(ctx, &items, `
        SELECT chrt_id, track_number, price, rid, name, sale,
               size, total_price, nm_id, brand, status
        FROM items WHERE order_uid = $1`, orderUID)
	if err == nil && len(items) > 0 {
		order.Items = &items
	} else if err != nil {
		return nil, fmt.Errorf("item select: %w", err)
	}

	return order, nil
}

func (r *Repo) Insert(ctx context.Context, order *Order) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return fmt.Errorf("начало транзакции: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
        INSERT INTO orders (
            order_uid, track_number, entry, locale, internal_signature,
            customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		order.OrderUID,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerID,
		order.DeliveryService,
		order.Shardkey,
		order.SmID,
		order.DateCreated,
		order.OofShard)

	if err != nil {
		if !isDuplicateKeyError(err) {
			return fmt.Errorf("вставка заказа: %w", err)
		}
		log.Printf("Заказ %s уже существует, пропускаем вставку", order.OrderUID)
	}

	if order.Delivery != nil {
		_, err = tx.ExecContext(ctx, `
            INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (order_uid) DO UPDATE SET
                name = EXCLUDED.name,
                phone = EXCLUDED.phone,
                zip = EXCLUDED.zip,
                city = EXCLUDED.city,
                address = EXCLUDED.address,
                region = EXCLUDED.region,
                email = EXCLUDED.email`,
			order.OrderUID,
			order.Delivery.Name,
			order.Delivery.Phone,
			order.Delivery.Zip,
			order.Delivery.City,
			order.Delivery.Address,
			order.Delivery.Region,
			order.Delivery.Email)
		if err != nil {
			return fmt.Errorf("вставка доставки: %w", err)
		}
	}

	if order.Payment != nil {
		_, err = tx.ExecContext(ctx, `
            INSERT INTO payments (order_uid, transaction, currency, provider, amount, 
                                payment_dt, bank, delivery_cost, goods_total, custom_fee)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (order_uid) DO UPDATE SET
                transaction = EXCLUDED.transaction,
                currency = EXCLUDED.currency,
                provider = EXCLUDED.provider,
                amount = EXCLUDED.amount,
                payment_dt = EXCLUDED.payment_dt,
                bank = EXCLUDED.bank,
                delivery_cost = EXCLUDED.delivery_cost,
                goods_total = EXCLUDED.goods_total,
                custom_fee = EXCLUDED.custom_fee`,
			order.OrderUID,
			order.Payment.Transaction,
			order.Payment.Currency,
			order.Payment.Provider,
			order.Payment.Amount,
			order.Payment.PaymentDt,
			order.Payment.Bank,
			order.Payment.DeliveryCost,
			order.Payment.GoodsTotal,
			order.Payment.CustomFee)
		if err != nil {
			return fmt.Errorf("вставка платежа: %w", err)
		}
	}

	if order.Items != nil {
		for _, item := range *order.Items {
			_, err = tx.ExecContext(ctx, `
                INSERT INTO items (order_uid, chrt_id, track_number, price, rid, 
                                 name, sale, size, total_price, nm_id, brand, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				order.OrderUID,
				item.ChrtID,
				item.TrackNumber,
				item.Price,
				item.Rid,
				item.Name,
				item.Sale,
				item.Size,
				item.TotalPrice,
				item.NmID,
				item.Brand,
				item.Status)
			if err != nil {
				return fmt.Errorf("вставка товара: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("коммит транзакции: %w", err)
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := r.redis.Set(ctx, order.OrderUID, jsonData, 1*time.Hour).Err(); err != nil {
		log.Printf("Ошибка Redis: %v", err)
	}

	return nil
}

func isDuplicateKeyError(err error) bool {
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code == "23505"
	}
	return false
}
