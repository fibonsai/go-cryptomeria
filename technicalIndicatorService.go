package main

import (
	"log"
)

type TechnicalIndicatorService struct {
	horizonSeconds  int
	rsiWindowSlider *WindowSlider
	stopCh          chan bool
}

type TrendForecast struct {
	willReverse      bool
	confidence       float32
	isDownTrend      bool
	trendStrengIndex float32
}

type TechIndicators struct {
	rsi float64
}

func NewTechnicalIndicatorService(horizonSeconds int) *TechnicalIndicatorService {
	return &TechnicalIndicatorService{
		horizonSeconds:  horizonSeconds,
		rsiWindowSlider: NewWindowSlider(60, 1000),
	}
}

func (tis *TechnicalIndicatorService) Start(handler func(trendForecast *TrendForecast, trade *Trade)) {
	go func() {
		for {
			select {
			case tradeWindow := <-tis.rsiWindowSlider.C():
				last := len(tradeWindow.timestamps) - 1
				trade := &Trade{
					asset:     tradeWindow.asset,
					timestamp: tradeWindow.timestamps[last],
					price:     tradeWindow.prices[last],
					amount:    tradeWindow.amounts[last],
				}
				rsi := tis.calculateRsi(tradeWindow)

				techIndicators := &TechIndicators{
					rsi: rsi,
				}
				trendForecast := tis.forecastTrendReversal(trade, techIndicators)
				handler(trendForecast, trade)
			case <-tis.stopCh:
				return
			}
		}
	}()
}

func (tis *TechnicalIndicatorService) Stop() {
	tis.stopCh <- true
}

func (tis *TechnicalIndicatorService) forecastTrendReversal(trade *Trade, techIndicators *TechIndicators) *TrendForecast {
	log.Printf("call TechInd to asset %s to check reverse until %d seconds", trade.asset, tis.horizonSeconds)

	// TODO
	return &TrendForecast{
		willReverse:      false,
		confidence:       1.0,
		isDownTrend:      true,
		trendStrengIndex: 1.0,
	}
}

func (tis *TechnicalIndicatorService) Update(trade *Trade) {
	tis.rsiWindowSlider.Update(trade)
}

func (tis *TechnicalIndicatorService) calculateRsi(tradeWindow *TradeWindow) float64 {
	// TODO
	return 0.0
}
