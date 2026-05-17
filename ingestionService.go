package main

import (
	"log"

	"gonum.org/v1/gonum/stat"
)

type IngestionService struct {
	windowSize int32
	SlopeMin   float32
	assets     map[string]*Asset
	debug      bool
}

type Asset struct {
	Prices map[int64]float64
	keys   []int64
}

func NewIngestionService(windowSize int32, slopeMin float32, debug bool) *IngestionService {
	return &IngestionService{
		windowSize: windowSize,
		SlopeMin:   slopeMin,
		assets:     make(map[string]*Asset),
		debug:      debug,
	}
}

func (is *IngestionService) onPriceTicket(trade *Trade, thresholdHandler func(threshold float64)) {
	timeSlot := int64(trade.timestamp / (60 * 1_000)) // convert timestamp to minutes

	anAsset, ok := is.assets[trade.asset]
	if !ok {
		anAsset = &Asset{
			Prices: make(map[int64]float64, is.windowSize+1),
			keys:   make([]int64, 0, is.windowSize+1),
		}
		is.assets[trade.asset] = anAsset

		anAsset.keys = append(anAsset.keys, timeSlot)
		anAsset.Prices[timeSlot] = trade.price
		return
	}

	lastTimeSlot := anAsset.keys[len(anAsset.keys)-1]
	if lastTimeSlot < timeSlot {
		anAsset.keys = append(anAsset.keys, timeSlot)
		anAsset.Prices[timeSlot] = trade.price

		if len(anAsset.keys) > int(is.windowSize) {
			for range len(anAsset.keys) - int(is.windowSize) {
				firstKey := anAsset.keys[0]
				delete(anAsset.Prices, firstKey)
				anAsset.keys = anAsset.keys[1:]
			}

			triggered, slope := is.isThresholdCalled(&anAsset.keys, &anAsset.Prices)
			if triggered {
				thresholdHandler(slope)
			}
		}
	}
}

func (is *IngestionService) isThresholdCalled(keys *[]int64, prices *map[int64]float64) (bool, float64) {
	var weights []float64
	avgX := make([]float64, 0, len(*keys))
	avgY := make([]float64, 0, len(*keys))

	for _, key := range *keys {
		price, ok := (*prices)[int64(key)]
		if ok {
			avgX = append(avgX, float64(key)-float64((*keys)[0]))
			avgY = append(avgY, price)
		}
	}

	_, beta := stat.LinearRegression(avgX, avgY, weights, false)

	if is.debug {
		log.Printf("[%d] slope: %f", len(*keys), beta)
	}

	return beta < float64(is.SlopeMin), beta
}
