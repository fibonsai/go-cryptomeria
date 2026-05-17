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
		technicalIndicatorService: NewTechnicalIndicatorService(),
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
	t.ticker = time.NewTicker(t.interval)
	defer t.ticker.Stop()

	for range t.ticker.C {
		t.technicalIndicatorService.forecastTrendReversal(t.asset, 60)

		timer := time.NewTimer(5 * time.Second)
		<-timer.C
		break
	}
}
