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
	closePrices map[int64]*ClosePrices
	keys        []int64
}

type ClosePrices struct {
	timestamp int64
	value     float64
}

func NewIngestionService(windowSize int32, slopeMin float32, debug bool) *IngestionService {
	return &IngestionService{
		windowSize: windowSize,
		SlopeMin:   slopeMin,
		assets:     make(map[string]*Asset),
		debug:      debug,
	}
}

func (is *IngestionService) onPriceTicket(asset string, price float64, timestamp int64, thresholdHandler func(threshold float64)) {
	timeSlot := int64(timestamp / (60 * 1_000)) // convert timestamp to minutes

	anAsset, ok := is.assets[asset]
	if !ok {
		anAsset = &Asset{
			closePrices: make(map[int64]*ClosePrices, is.windowSize+1),
			keys:        make([]int64, 0, is.windowSize+1),
		}
		is.assets[asset] = anAsset

		anAsset.keys = append(anAsset.keys, timeSlot)
		anAsset.closePrices[timeSlot] = &ClosePrices{timestamp: timeSlot, value: price}
		return
	}

	lastTimeSlot := anAsset.keys[len(anAsset.keys)-1]
	if lastTimeSlot < timeSlot {
		anAsset.keys = append(anAsset.keys, timeSlot)
		anAsset.closePrices[timeSlot] = &ClosePrices{timestamp: timeSlot, value: price}

		if len(anAsset.keys) > int(is.windowSize) {
			for range len(anAsset.keys) - int(is.windowSize) {
				firstKey := anAsset.keys[0]
				delete(anAsset.closePrices, firstKey)
				anAsset.keys = anAsset.keys[1:]
			}

			triggered, slope := is.isThresholdCalled(&anAsset.keys, &anAsset.closePrices)
			if triggered {
				thresholdHandler(slope)
			}
		}
	}
}

func (is *IngestionService) isThresholdCalled(keys *[]int64, closePrices *map[int64]*ClosePrices) (bool, float64) {
	var weights []float64
	avgX := make([]float64, 0, len(*keys))
	avgY := make([]float64, 0, len(*keys))

	for _, key := range *keys {
		closePrice, ok := (*closePrices)[int64(key)]
		if ok {
			avgX = append(avgX, float64(closePrice.timestamp)-float64((*closePrices)[(*keys)[0]].timestamp))
			avgY = append(avgY, closePrice.value)
		}
	}

	_, beta := stat.LinearRegression(avgX, avgY, weights, false)

	if is.debug {
		log.Printf("[%d] slope: %f", len(*keys), beta)
	}

	return beta < float64(is.SlopeMin), beta
}
