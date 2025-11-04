Steps to run : 

1. Clone repo

2. Install docker daeomon and leave it running

3. cd to project directory and docker compose up -d

The ingestion worker fetches market data from the public Binance 24 hour ticker
endpoint (`https://data.binance.com/api/v3/ticker/24hr`) and publishes the
normalized payloads to Kafka. You can override the endpoint with the
`BINANCE_ENDPOINT` environment variable or provide a comma separated list of
symbols to keep via `BINANCE_SYMBOLS` if you want to focus on a subset of the
available tickers.
