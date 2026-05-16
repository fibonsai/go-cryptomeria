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

	subject := "trade"

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChannel := make(chan os.Signal, 1)
		signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
		<-sigChannel
		close(sigChannel)
		cancel()
	}()

	marketData = NewMarketDataParquet(filepath, subject, ctx)
	ingestionService := NewIngestionService(60, -40.0, false)

	go marketData.Subscriber(func(counter int, subject string, trade *TradeDao) {
		if debug {
			log.Printf("%d %s %s", counter, subject, trade)
		}
		ingestionService.onPriceTicket(subject, trade.Price, trade.Timestamp, func(threshold float64) {
			log.Printf("threshold called at %d. Slope is %f", trade.Timestamp, threshold)
		})
	})

	go marketData.Start()

	<-ctx.Done()

	marketData.Stop()
}
