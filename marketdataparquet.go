package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"google.golang.org/protobuf/proto"
)

const READER_DELAY = 0
const DATA_CHANNEL_BUFFER_SIZE = 1000000

type MarketDataParquet struct {
	filepath string
	asset    string
	ctx      context.Context
	dataCh   chan *[]byte
}

func NewMarketDataParquet(filepath string, asset string, ctx context.Context) *MarketDataParquet {
	ch := make(chan *[]byte, DATA_CHANNEL_BUFFER_SIZE)

	return &MarketDataParquet{
		filepath: filepath,
		asset:    asset,
		ctx:      ctx,
		dataCh:   ch,
	}
}

func (mdp *MarketDataParquet) getNumRows(db *sql.DB) int32 {
	queryCount := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", mdp.filepath)
	var numRows int32 = 0
	countResult := db.QueryRow(queryCount)
	if err := countResult.Scan(&numRows); err != nil {
		log.Fatal(err)
		return 0
	}
	return numRows
}

func (mdp *MarketDataParquet) Stop() {
	close(mdp.dataCh)
}

func (mdp *MarketDataParquet) Subscriber(handler func(counter int, asset string, trade *Trade)) {

	counter := 0

	for {
		select {
		case <-mdp.ctx.Done():
			log.Println("exiting from consumer")
			return

		case m := <-mdp.dataCh:
			{
				tradeDao := &TradeDao{}
				if err := proto.Unmarshal(*m, tradeDao); err != nil {
					log.Fatal(err)
				}
				counter++
				handler(counter, mdp.asset, &Trade{
					asset:     mdp.asset,
					timestamp: tradeDao.Timestamp,
					side:      tradeDao.Side,
					id:        tradeDao.Id,
					price:     tradeDao.Price,
					amount:    tradeDao.Amount,
				})
			}
		}
	}
}

func (mdp *MarketDataParquet) Start() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}

	numRows := mdp.getNumRows(db)
	log.Printf("num rows: %d", numRows)

	query := fmt.Sprintf("SELECT timestamp, side, id, price, amount FROM read_parquet('%s')", mdp.filepath)
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
	}()

	counter := 0
	for {
		select {
		case <-mdp.ctx.Done():
			log.Println("exiting from producer")
			log.Printf("processed %d rows", counter)
			return
		default:
			{
				if rows.Next() {
					if READER_DELAY > 0 {
						time.Sleep(READER_DELAY)
					}
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
					mdp.dataCh <- &tradeRaw
				} else {
					log.Printf("All rows processed. Total %d rows", counter)
					log.Println("exiting from producer")
					return
				}
			}
		}
	}

}
