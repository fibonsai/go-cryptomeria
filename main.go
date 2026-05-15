package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func main() {
	args := os.Args
	if len(args) != 2 {
		log.Fatal("argument missing.")
		return
	}
	filePath := args[1]

	// 2. Initialize Database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	errCh := make(chan error, 1)
	numRows := getNumRows(filePath, db)
	log.Printf("num rows: %d", numRows)

	var wg sync.WaitGroup
	wg.Add(numRows)

	nc := newNatsConn(&wg, errCh)
	defer nc.Close()

	subject := "trade"
	sub := subscribe(nc, subject, &wg)

	if err := publish(filePath, db, nc, subject); err != nil {
		log.Fatal(err)
	}

	if err := nc.Drain(); err != nil {
		log.Fatal(err)
	}

	wg.Wait()

	if !nc.IsClosed() {
		if err := sub.Unsubscribe(); err != nil {
			log.Fatal(err)
		}
	}

	select {
	case e := <-errCh:
		log.Fatal(e)
	default:
	}
}

func getNumRows(filePath string, db *sql.DB) int {
	queryCount := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", filePath)
	numRows := 0
	countResult := db.QueryRow(queryCount)
	if err := countResult.Scan(&numRows); err != nil {
		log.Fatal(err)
		return 0
	}
	return numRows
}

func newNatsConn(wg *sync.WaitGroup, errCh chan error) *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL,
		nats.ConnectHandler(func(c *nats.Conn) {
			wg.Add(1)
		}),
		// ATTENTION: large buffer may drop messages
		nats.WriteBufferSize(10),
		nats.DrainTimeout(10*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh <- err
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			wg.Done()
		}))
	if err != nil {
		log.Fatal(err)
	}
	return nc
}

func subscribe(nc *nats.Conn, subject string, wg *sync.WaitGroup) *nats.Subscription {
	var counter atomic.Int32
	counter.Store(0)

	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		trade := &TradeDao{}
		defer wg.Done()
		if err := proto.Unmarshal(m.Data, trade); err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("%d %s %s", counter.Add(1), m.Subject, trade)
	})
	if err != nil {
		log.Fatal(err)
	}
	return sub
}

func publish(filePath string, db *sql.DB, nc *nats.Conn, subject string) error {
	query := fmt.Sprintf("SELECT timestamp, side, id, price, amount FROM read_parquet('%s')", filePath)
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Query error: %v", err)
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	counter := 0
	for rows.Next() {
		trade := &TradeDao{}
		if err := rows.Scan(&trade.Timestamp, &trade.Side, &trade.Id, &trade.Price, &trade.Amount); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}

		tradeRaw, err := proto.Marshal(trade)
		if err != nil {
			log.Fatal(err)
			continue
		}

		counter++
		if err := nc.Publish(subject, tradeRaw); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("processed %d rows", counter)
	return nil
}
