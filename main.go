package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var VarLabNatsUrl string = "nats://10.20.0.100:4222"

// {"ticker_slug":"KXEFLCHAMPIONSHIPGAME-25DEC13NORSOU-SOU","event_ticker":"KXEFLCHAMPIONSHIPGAME-25DEC13NORSOU","price":45,"volume":207,"side":"yes","timestamp":"2025-12-13T13:05:42Z","msg_type":"trade"}
type KalshiTrade struct {
	TickerSlug   string  `json:"ticker_slug"`
	EventTicker  string  `json:"event_ticker"`
	Price        *int64  `json:"price,omitempty"`
	Volume       *int64  `json:"volume,omitempty"`
	Side         *string `json:"side,omitempty"`
	Timestamp    string  `json:"timestamp"`
	MsgType      string  `json:"msg_type"` // "ticker" or "trade"
}

func main() {
	fmt.Println("tfclient starting...")
	streamName := "KALSHI_TRADES"

	nc, err := nats.Connect(VarLabNatsUrl)
	if err != nil {
		fmt.Printf("Error connecting to NATS: %v\n", err)
		return
	}
	fmt.Println("Connected to NATS server at", VarLabNatsUrl)
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("Error creating JetStream context: %v\n", err)
		return
	}

	ctx := context.Background()

	// Create durable consumer
	c, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "TheGoConsumer",
		Name:          "TheGoConsumer",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	fmt.Printf("Subscribed to stream %s\n", streamName)

	for {
		// Fetch 1 message with short timeout for lower latency
		msgs, err := c.Fetch(8, jetstream.FetchMaxWait(50*time.Millisecond))
		if err != nil {
			if err == context.DeadlineExceeded || err.Error() == "nats: timeout" {
				continue // No message available, try again
			}
			fmt.Printf("Error fetching: %v\n", err)
			continue
		}

		for msg := range msgs.Messages() {
			var trade KalshiTrade
			err = json.Unmarshal(msg.Data(), &trade)
			if err != nil {
				fmt.Printf("Error unmarshaling: %v\n", err)
				msg.Ack()
				continue
			}

			md, _ := msg.Metadata()
			fmt.Printf("%s (%d) => ticker: %s, event ticker: %s\n", msg.Subject(), md.Sequence.Stream, trade.TickerSlug, trade.EventTicker)
			if trade.MsgType == "trade" && trade.Price != nil && trade.Volume != nil && trade.Side != nil {
				fmt.Printf("    price: %d, volume: %d, side: %s, timestamp: %s\n", *trade.Price, *trade.Volume, *trade.Side, trade.Timestamp)
			} else {
				fmt.Printf("    msg_type: %s, timestamp: %s\n", trade.MsgType, trade.Timestamp)
			}
			msg.Ack()
		}
	}
}
