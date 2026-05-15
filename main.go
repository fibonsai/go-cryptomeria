package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const READER_DELAY = 1000 * time.Nanosecond
const DATA_CHANNEL_BUFFER_SIZE = 1000000

func main() {
	args := os.Args
	if len(args) != 2 {
		log.Fatal("argument missing.")
		return
	}
	filePath := args[1]

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChannel := make(chan os.Signal, 1)
		signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
		<-sigChannel
		close(sigChannel)
		cancel()
	}()

	subject := "trade"

	go subscriber(ctx, subject, func(counter int, subject string, trade *TradeDao) {
		log.Printf("%d %s %s", counter, subject, trade)
	})

	go parquetReader(ctx, filePath, subject)

	<-ctx.Done()

}

func getNumRows(filePath string, db *sql.DB) int32 {
	queryCount := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", filePath)
	var numRows int32 = 0
	countResult := db.QueryRow(queryCount)
	if err := countResult.Scan(&numRows); err != nil {
		log.Fatal(err)
		return 0
	}
	return numRows
}

func newNatsConn() *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL,
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Fatal(err)
		}))
	if err != nil {
		log.Fatal(err)
	}
	return nc
}

func subscriber(ctx context.Context, subject string, handler func(c int, subj string, t *TradeDao)) {
	nc := newNatsConn()
	defer nc.Close()
	dataCh := make(chan *nats.Msg, DATA_CHANNEL_BUFFER_SIZE)
	sub, err := nc.ChanSubscribe(subject, dataCh)
	if err != nil {
		log.Fatal("Failed to subscribe to subject:", err)
	}
	defer func() {
		sub.Unsubscribe()
		close(dataCh)
		if err := nc.Drain(); err != nil {
			log.Fatal(err)
		}
	}()

	counter := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("exiting from consumer")
			return

		case m := <-dataCh:
			{
				trade := &TradeDao{}
				if err := proto.Unmarshal(m.Data, trade); err != nil {
					log.Fatal(err)
				}
				counter++
				handler(counter, m.Subject, trade)
			}
		}
	}
}

func parquetReader(ctx context.Context, filePath string, subject string) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}

	numRows := getNumRows(filePath, db)
	log.Printf("num rows: %d", numRows)

	nc := newNatsConn()
	query := fmt.Sprintf("SELECT timestamp, side, id, price, amount FROM read_parquet('%s')", filePath)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Query error: %v", err)
	}
	
	defer func() {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
		if err := nc.Drain(); err != nil {
			log.Fatal(err)
		}
	}()

	counter := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("exiting from producer")
			log.Printf("processed %d rows", counter)
			return
		default:
			{
				if rows.Next() {
					time.Sleep(READER_DELAY)
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
				} else {
					log.Printf("All rows processed. Total %d rows", counter)
					log.Println("exiting from producer")
					return
				}
			}
		}
	}

}
