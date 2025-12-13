package main

import (
	"context"
	"fmt"
	"os"
	"encoding/json"
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
	var streamName string = "KALSHI_TRADES"
	if len(os.Args) > 1 {
		fmt.Printf("Args: %v\n", os.Args[1:])
	}

	nc, err := nats.Connect(VarLabNatsUrl)
	if err != nil {
		fmt.Printf("Error connecting to NATS: %v\n", err)
		return
	}
	fmt.Println("Connected to NATS server at", VarLabNatsUrl)
	defer nc.Close()

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("Error creating JetStream context: %v\n", err)
		return
	}

	ctx := context.Background()

	c, err := js.CreateOrUpdateConsumer(ctx, streamName,jetstream.ConsumerConfig{
			Durable:   "TheGoConsumer", // Durable name
			Name:  	   "TheGoConsumer", // must match Durable
			// FilterSubject: "kalshi.trades.KXEPLSPREAD-25DEC13LFCBRI-LFC1",
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	cons, err := c.Consume(func(msg jetstream.Msg) {
		var trade KalshiTrade
		err := json.Unmarshal(msg.Data(), &trade)
		if err != nil {
			fmt.Printf("Error unmarshaling message: %v\n", err)
			msg.Ack() // Acknowledge to avoid redelivery
			return
		}
		msg.Ack()
		 md, err := msg.Metadata()
		if err != nil {
		 	fmt.Printf("Error getting message metadata: %v\n", err)
		 	return
		}
		fmt.Printf("%s (%d) => ticker: %s, event ticker: %s\n", msg.Subject(), md.Sequence.Stream, trade.TickerSlug, trade.EventTicker)
		fmt.Printf("    price: %v, volume: %v, side: %v, timestamp: %s, msg_type: %s\n", *trade.Price, *trade.Volume, *trade.Side, trade.Timestamp, trade.MsgType)
	})

	if err != nil {
		fmt.Printf("Error subscribing to subject: %v\n", err)
		return
	}
	defer cons.Stop()

	fmt.Printf("Subscribed to stream %s\n", streamName)
	<-ctx.Done()
}
