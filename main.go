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

	go marketData.Subscriber(func(counter int, subject string, trade *TradeDao) {
		log.Printf("%d %s %s", counter, subject, trade)
	})

	go marketData.Start()

	<-ctx.Done()

	marketData.Stop()
}
