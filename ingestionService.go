package main

import (
	"log"

	"gonum.org/v1/gonum/stat"
)

type IngestionService struct {
	windowSize   int32
	SlopeMin     float64
	debug        bool
	windowSlider *WindowSlider
	stopCh       chan bool
}

type Asset struct {
	Prices map[int64]float64
	keys   []int64
}

func NewIngestionService(windowSize int32, slopeMin float64, debug bool) *IngestionService {
	return &IngestionService{
		windowSize:   windowSize,
		SlopeMin:     slopeMin,
		debug:        debug,
		windowSlider: NewWindowSlider(int(windowSize), 60_000),
		stopCh:       make(chan bool, 1),
	}
}

func (is *IngestionService) Start(thresholdHandler func(trade *Trade, threshold float64)) {
	go func() {
		for {
			select {
			case tradeWindow := <-is.windowSlider.C():
				last := len(tradeWindow.timestamps) - 1
				trade := &Trade{
					asset:     tradeWindow.asset,
					timestamp: tradeWindow.timestamps[last],
					price:     tradeWindow.prices[last],
					amount:    tradeWindow.amounts[last],
				}
				beta := is.calculateSlope(tradeWindow)
				if beta < is.SlopeMin {
					thresholdHandler(trade, beta)
				}
			case <-is.stopCh:
				return
			}
		}
	}()
}

func (is *IngestionService) Stop() {
	// drain channel
loop:
	for {
		select {
		case <-is.windowSlider.C():
		default:
			break loop
		}
	}
	is.windowSlider.Stop()
	is.stopCh <- true
}

func (is *IngestionService) onPriceTicket(trade *Trade) {
	is.windowSlider.Update(trade)
}

func (is *IngestionService) calculateSlope(tradeWindow *TradeWindow) float64 {
	var weights []float64
	posX := tradeWindow.seqs
	prices := tradeWindow.prices

	_, beta := stat.LinearRegression(posX, prices, weights, false)

	if is.debug {
		log.Printf("[%d] slope: %f, x: %v y: %v", len(posX), beta, posX, prices)
	}

	return beta
}
