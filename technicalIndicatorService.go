package main

import "log"

type TechnicalIndicatorService struct {
	dataCh chan *Trade
}

type TrendForecast struct {
	willReverse      bool
	confidence       float32
	isDownTrend      bool
	trendStrengIndex float32
}

func NewTechnicalIndicatorService() *TechnicalIndicatorService {
	dataCh := make(chan *Trade, 1000)
	go func() {
		for data := range dataCh {
			log.Printf("%s", data)
		}
	}()

	return &TechnicalIndicatorService{
		dataCh: dataCh,
	}
}

func (tis *TechnicalIndicatorService) forecastTrendReversal(asset string, horizonSeconds int) *TrendForecast {
	log.Printf("call TechInd to asset %s to check reverse until %d seconds", asset, horizonSeconds)

	// TODO
	return &TrendForecast{
		willReverse:      false,
		confidence:       1.0,
		isDownTrend:      true,
		trendStrengIndex: 1.0,
	}
}

func (tis *TechnicalIndicatorService) update(trade *Trade) {
	select {
	case tis.dataCh <- trade:
		log.Println("buffered")
	default:
		log.Println("dropped")
	}
}
