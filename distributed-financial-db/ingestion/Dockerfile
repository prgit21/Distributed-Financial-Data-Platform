FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py ./

CMD ["python", "worker.py"]




# CID=$(docker compose ps -q ingestion-worker)
# NET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "$CID" | head -n1)
# HOST=data-api.binance.vision


# docker run --rm --network "$NET" python:3.11-alpine sh -lc '
# set -e
# apk add --no-cache curl jq >/dev/null
# SYMS=$(curl -sS "https://'"$HOST"'/api/v3/exchangeInfo?permissions=SPOT" \
#   | jq -r '"'"'.symbols
#     | map(select(.status=="TRADING" and .quoteAsset=="USDT"))
#     | map(.symbol)
#     | map(select(test("(UP|DOWN)$")==false))    # drop leveraged tokens
#     | unique | .[:100] | @json'"'"')
# curl -sS -G "https://'"$HOST"'/api/v3/ticker/24hr" --data-urlencode symbols="$SYMS" \
#  | jq -c ".[0:3]"   # show first 3 items only
# '
