package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"os"
	"os/signal"

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
	var last_trade map[string]KalshiTrade = make(map[string]KalshiTrade)

	// var myArray [16]int64
	// myArray[0] = 42
	// mySlice := myArray[:8]
	// fmt.Println("mySlice:", mySlice)
	var done bool = false
	var lastSeq uint64 = 0

	// read last seq from file
	if data, err := os.ReadFile("last_seq.txt"); err == nil {
		_, err := fmt.Sscanf(string(data), "%d", &lastSeq)
		if err != nil {
			fmt.Printf("Error reading last_seq.txt: %v\n", err)
		} else {
			fmt.Printf("Resuming from last sequence: %d\n", lastSeq)
		}
	}
	var pendingCount uint64= 0

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	go func(){
			for sig := range sigchan {
				fmt.Println("Received signal:", sig)
				done = true
			}
	}()


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
	var fetchSize int = 32
	var conInfo* jetstream.ConsumerInfo = c.CachedInfo()
	if conInfo != nil {
		fmt.Printf("Consumer info: Delivered %d, Acked %d, Pending %d\n",
			conInfo.Delivered.Stream,
			conInfo.AckFloor.Stream,
			conInfo.NumPending,
		)
		pendingCount = conInfo.NumPending
		if conInfo.NumPending > 1000 {
			fetchSize = 1024
			fmt.Printf("Setting fetch size to %d due to pending messages\n", fetchSize)
		}
	}

	var msgCount uint64 = 0

	for done == false {

		// Fetch 1 message with short timeout for lower latency
		msgs, err := c.Fetch(fetchSize, jetstream.FetchMaxWait(50*time.Millisecond))
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
			last_trade[trade.TickerSlug] = trade

			md, _ := msg.Metadata()
			if lastSeq == 0 {
				fmt.Printf("Starting at sequence %d\n", md.Sequence.Stream)
			}
			if (lastSeq + 1) != md.Sequence.Stream {
				fmt.Printf("WARNING: Detected gap in sequence! lastSeq: %d, currentSeq: %d\n", lastSeq, md.Sequence.Stream)
			}
			lastSeq = md.Sequence.Stream

			// fmt.Printf("%s (%d) => ticker: %s, event ticker: %s\n", msg.Subject(), md.Sequence.Stream, trade.TickerSlug, trade.EventTicker)
			// if trade.MsgType == "trade" && trade.Price != nil && trade.Volume != nil && trade.Side != nil {
			// 	fmt.Printf("    price: %d, volume: %d, side: %s, timestamp: %s\n", *trade.Price, *trade.Volume, *trade.Side, trade.Timestamp)
			// } else {
			// 	fmt.Printf("    msg_type: %s, timestamp: %s\n", trade.MsgType, trade.Timestamp)
			// }
			msg.Ack()
			msgCount++
			if msgCount%256 == 0 {
				fmt.Printf("Processed %d messages last seq %d keys %d\n", msgCount, md.Sequence.Stream, len(last_trade))

				// if startSeq > 0 && (startSeq+msgCount) >= lastSeq {
				// 	fetchSize = 32
				// 	fmt.Println("Slowing fetch size to 32...	")
				// }
				if pendingCount > 0 && msgCount > pendingCount {
					fetchSize = 32
					fmt.Println("Slowing fetch size to 32...	")
					pendingCount = 0
				}
			}
		}
	}

	fmt.Println("Shutting down tfclient...")
	fmt.Println("Final processed message count:", msgCount)
	fmt.Println("Final last sequenece:", lastSeq)

	// write last seq to file
	f, err := os.Create("last_seq.txt")
	if err != nil {
		fmt.Printf("Error creating last_seq.txt: %v\n", err)
		return
	}
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("%d\n", lastSeq))
	if err != nil {
		fmt.Printf("Error writing to last_seq.txt: %v\n", err)
		return
	}
}
