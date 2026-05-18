package main

import (
	"log"
	"time"
)

// ##############
type PreTradeService struct {
	intervalToCheck           time.Duration
	assetsMonitored           map[string]*TickerManager
	technicalIndicatorService *TechnicalIndicatorService
}

func NewPreTradeService(intervalToCheck time.Duration) *PreTradeService {
	return &PreTradeService{
		intervalToCheck:           intervalToCheck,
		assetsMonitored:           make(map[string]*TickerManager),
		technicalIndicatorService: NewTechnicalIndicatorService(60),
	}
}

func (pts *PreTradeService) StartMonitor(refTrade *Trade) {
	defer delete(pts.assetsMonitored, refTrade.asset)

	assetMonitored, ok := pts.assetsMonitored[refTrade.asset]
	if !ok {
		assetMonitored = NewTickerManager(refTrade.asset, 10, pts.technicalIndicatorService)
		pts.assetsMonitored[refTrade.asset] = assetMonitored
	} else {
		log.Printf("asset %s alread monitored. Aborting request", refTrade.asset)
		return
	}

	assetMonitored.Start()
}

func (pts *PreTradeService) Update(trade *Trade) {
	pts.technicalIndicatorService.Update(trade)
}

// ##############
type TickerManager struct {
	asset                     string
	interval                  time.Duration
	ticker                    *time.Ticker
	technicalIndicatorService *TechnicalIndicatorService
}

func NewTickerManager(asset string, interval time.Duration, technicalIndicatorService *TechnicalIndicatorService) *TickerManager {
	return &TickerManager{
		asset:                     asset,
		interval:                  interval * time.Second,
		technicalIndicatorService: technicalIndicatorService,
	}
}

func (t *TickerManager) Start() {
	t.technicalIndicatorService.Start(func(trendForecast *TrendForecast, trade *Trade) {
		if trendForecast.willReverse && trendForecast.confidence > 0.8 && trendForecast.isDownTrend && trendForecast.trendStrengIndex < 0.2 {
			// lets do it
		}
	})
}
