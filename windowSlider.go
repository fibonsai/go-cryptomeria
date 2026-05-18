package main

type WindowSlider struct {
	windowSize int
	slotSize   int64
	keys       []int64
	trades     map[int64]*Trade
	channel    chan *TradeWindow
}

type TradeWindow struct {
	asset      string
	seqs       []float64
	timestamps []int64
	prices     []float64
	amounts    []float64
}

func NewWindowSlider(windowSize int, slotSize int64) *WindowSlider {
	return &WindowSlider{
		windowSize: windowSize,
		slotSize:   slotSize,
		keys:       make([]int64, 0, windowSize),
		trades:     make(map[int64]*Trade, windowSize),
		channel:    make(chan *TradeWindow, 1000),
	}
}

func (ws *WindowSlider) Update(trade *Trade) {
	timeSlot := int64(trade.timestamp / ws.slotSize) // convert timestamp in 'ms' to slotSize

	if len(ws.keys) == 0 {
		ws.keys = append(ws.keys, timeSlot)
		ws.trades[timeSlot] = trade
		return
	}

	lastTimeSlot := ws.keys[len(ws.keys)-1]
	if lastTimeSlot < timeSlot {
		ws.keys = append(ws.keys, timeSlot)
		ws.trades[timeSlot] = trade

		if len(ws.keys) > ws.windowSize {
			for range len(ws.keys) - ws.windowSize {
				firstKey := ws.keys[0]
				delete(ws.trades, firstKey)
				ws.keys = ws.keys[1:]
			}

			ws.processWindow(trade.asset)
		}
	}
}

func (ws *WindowSlider) processWindow(asset string) {
	seqs := make([]float64, 0, len(ws.keys))
	timestamps := make([]int64, 0, len(ws.keys))
	prices := make([]float64, 0, len(ws.keys))
	amounts := make([]float64, 0, len(ws.keys))

	for _, key := range ws.keys {
		trade, ok := ws.trades[int64(key)]
		if ok {
			seqs = append(seqs, float64(key-ws.keys[0]))
			timestamps = append(timestamps, trade.timestamp)
			prices = append(prices, trade.price)
			amounts = append(amounts, trade.amount)
		}
	}

	tradeWindow := &TradeWindow{
		asset:      asset,
		seqs:       seqs,
		timestamps: timestamps,
		prices:     prices,
		amounts:    amounts,
	}

	ws.channel <- tradeWindow
}

func (ws *WindowSlider) C() <-chan *TradeWindow {
	return ws.channel
}

func (ws *WindowSlider) Stop() {
	close(ws.channel)
}
