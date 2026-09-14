import json
import boto3
import websocket

AWS_PROFILE = "stock-de"
AWS_REGION = "ap-southeast-7"
SECRET_NAME = "stock-market-data-platform/finnhub"
KINESIS_STREAM_NAME = "stock-trades-stream"


def get_finnhub_api_key():
    session = boto3.Session()

    client = session.client(
        "secretsmanager",
        region_name=AWS_REGION
    )

    response = client.get_secret_value(
        SecretId=SECRET_NAME
    )

    secret = json.loads(response["SecretString"])

    return secret["FINNHUB_API_KEY"]


def get_kinesis_client():
    session = boto3.Session()

    return session.client(
        "kinesis",
        region_name=AWS_REGION
    )


kinesis_client = get_kinesis_client()


def send_trade_to_kinesis(trade):
    symbol = trade["s"]

    record = {
        "symbol": symbol,
        "price": trade["p"],
        "timestamp": trade["t"],
        "volume": trade["v"],
        "source": "finnhub"
    }

    response = kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(record).encode("utf-8"),
        PartitionKey=symbol
    )

    print(
        f"Sent {symbol} | "
        f"price={record['price']} | "
        f"shard={response['ShardId']}"
    )


def on_message(ws, message):
    payload = json.loads(message)

    if payload.get("type") != "trade":
        return

    trades = payload.get("data", [])

    for trade in trades:
        send_trade_to_kinesis(trade)


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")
    print(f"Status: {close_status_code}")
    print(f"Message: {close_msg}")


def on_open(ws):
    print("Connected to Finnhub WebSocket")

    symbols = ["AAPL"]

    for symbol in symbols:
        ws.send(
            json.dumps({
                "type": "subscribe",
                "symbol": symbol
            })
        )

        print(f"Subscribed to {symbol}")


if __name__ == "__main__":
    api_key = get_finnhub_api_key()

    print("Finnhub API key loaded successfully")

    websocket_url = f"wss://ws.finnhub.io?token={api_key}"

    ws = websocket.WebSocketApp(
        websocket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()