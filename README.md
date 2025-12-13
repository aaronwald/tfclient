# tfclient

Go client for publishing Kalshi trades to NATS JetStream.

## Setup

```bash
go mod tidy
go build -o tfclient .
```

## NATS CLI

Install the NATS CLI:

```bash
go install github.com/nats-io/natscli/nats@latest
export PATH="$PATH:$HOME/go/bin"
```

List streams:

```bash
nats -s nats://10.20.0.100:4222 stream ls
nats -s nats://10.20.0.100:4222 stream info KALSHI_TRADES
nats -s nats://10.20.0.100:4222 stream view KALSHI_TRADES
```

## JetStream Configuration

- Stream: `KALSHI_TRADES`
- Subjects: `kalshi.trades.>`
- Retention: 15 minutes
- Storage: File (persisted)
