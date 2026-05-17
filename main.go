package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type MarketData interface {
	Subscriber(func(counter int, subject string, trade *Trade))
	Start()
	Stop()
}

type Trade struct {
	asset     string
	timestamp int64
	side      string
	id        string
	price     float64
	amount    float64
}

func (t *Trade) String() string {
	return fmt.Sprintf("{asset: %s, timestamp: %d, side: %s, id: %s, price: %f, amount: %f}", t.asset, t.timestamp, t.side, t.id, t.price, t.amount)
}

var marketData MarketData

func main() {
	args := os.Args
	if len(args) != 2 {
		log.Fatal("argument missing.")
		return
	}
	filepath := args[1]

	debug := false

	asset := "BTCUSD"

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChannel := make(chan os.Signal, 1)
		signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
		<-sigChannel
		close(sigChannel)
		cancel()
	}()

	marketData = NewMarketDataParquet(filepath, asset, ctx)
	ingestionService := NewIngestionService(60, -40.0, false)
	preTradeService := NewPreTradeService(10)
	technicalIndicatorService := NewTechnicalIndicatorService()

	go marketData.Subscriber(func(counter int, asset string, trade *Trade) {
		if debug {
			log.Printf("%d %s", counter, trade)
		}
		technicalIndicatorService.update(trade)

		ingestionService.onPriceTicket(trade, func(threshold float64) {
			if debug {
				log.Printf("[%d] Slope is %f. Reference Price is %f to Asset %s", trade.timestamp, threshold, trade.price, trade.asset)
			}
			preTradeService.StartMonitor(trade)
		})
	})

	go marketData.Start()

	<-ctx.Done()

	marketData.Stop()
}
