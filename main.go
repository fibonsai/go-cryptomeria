package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type MarketData interface {
	Subscriber(func(counter int, subject string, trade *TradeDao))
	Start()
	Stop()
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

	go marketData.Subscriber(func(counter int, asset string, trade *TradeDao) {
		if debug {
			log.Printf("%d %s %s", counter, asset, trade)
		}
		ingestionService.onPriceTicket(asset, trade.Price, trade.Timestamp, func(threshold float64) {
			log.Printf("[%d] Slope is %f. Reference Price is %f to Asset %s", trade.Timestamp, threshold, trade.Price, asset)
		})
	})

	go marketData.Start()

	<-ctx.Done()

	marketData.Stop()
}
