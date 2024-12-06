from flask import Flask
from flask_socketio import SocketIO, emit
import websocket
import json
import pandas as pd
import pytz
import threading
from flask import Flask, render_template

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", logger=True, engineio_logger=True)

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))  # Heroku provides the PORT environment variable
    app.run(host="0.0.0.0", port=port)

# Existing Binance WebSocket
binance_socket = 'wss://fstream.binance.com/ws/!forceOrder@arr'

# Added Kraken, Coinbase, and OKX WebSockets
kraken_socket = 'wss://ws.kraken.com'
coinbase_socket = 'wss://ws-feed.pro.coinbase.com'
okx_socket = 'wss://ws.okx.com:8443/ws/v5/public'

# Filters (set to show all data by default)
minimum_total_dollars = 750
set_ticker = None

def on_message(ws, message):
    try:
        data = json.loads(message)
        source = getattr(ws, 'exchange_name', 'Unknown')
        new_row = None

        if source == 'Binance':
            # Binance forced liquidation data
            order_data = data['o']
            trade_time = pd.to_datetime(order_data['T'], unit='ms', utc=True)
            tz = pytz.timezone('Asia/Seoul')
            trade_time = trade_time.tz_convert(tz)
            new_row = {
                'Symbol': order_data['s'],
                'Side': order_data['S'],
                'Price': float(order_data['p']),
                'Quantity': float(order_data['q']),
                'Total($)': round(float(order_data['p']) * float(order_data['q']), 2),
                'Trade Time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                'Source': 'Binance'
            }

        elif source == 'Kraken':
            # Kraken trades subscription will send arrays of trades
            # Example message format: [channelID, [ [price, volume, time, side, orderType, misc] ], "trade", "XBT/USD"]
            # We'll assume we got a trade message:
            if isinstance(data, list) and len(data) > 3 and data[2] == 'trade':
                trades = data[1]
                # We'll just process the first trade in the list for demonstration
                for t in trades:
                    price = float(t[0])
                    qty = float(t[1])
                    timestamp = float(t[2])
                    side = 'Buy' if t[3] == 'b' else 'Sell'
                    trade_time = pd.to_datetime(timestamp, unit='s', utc=True)
                    tz = pytz.timezone('Asia/Seoul')
                    trade_time = trade_time.tz_convert(tz)
                    new_row = {
                        'Symbol': data[3] if len(data) > 3 else 'Unknown',
                        'Side': side,
                        'Price': price,
                        'Quantity': qty,
                        'Total($)': round(price * qty, 2),
                        'Trade Time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'Source': 'Kraken'
                    }
                    break  # process just one trade per message for simplicity

        elif source == 'Coinbase':
            # Coinbase 'matches' channel trade format
            # Example match message:
            # {
            #   "type": "match",
            #   "trade_id": 10,
            #   "maker_order_id": "...",
            #   "taker_order_id": "...",
            #   "side": "buy",
            #   "size": "0.005",
            #   "price": "100.23",
            #   "product_id": "BTC-USD",
            #   "sequence": 130,
            #   "time": "2020-07-06T20:58:53.554555Z"
            # }
            if data.get('type') == 'match':
                side = 'Buy' if data['side'] == 'buy' else 'Sell'
                price = float(data['price'])
                qty = float(data['size'])
                trade_time = pd.to_datetime(data['time'], utc=True)
                tz = pytz.timezone('Asia/Seoul')
                trade_time = trade_time.tz_convert(tz)
                new_row = {
                    'Symbol': data['product_id'],
                    'Side': side,
                    'Price': price,
                    'Quantity': qty,
                    'Total($)': round(price * qty, 2),
                    'Trade Time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Source': 'Coinbase'
                }

        elif source == 'OKX':
            # OKX trades example
            # {
            #   "arg":{"channel":"trades","instId":"BTC-USDT"},
            #   "data":[{"instId":"BTC-USDT","tradeId":"...","px":"...","sz":"...","side":"buy","ts":"..."}]
            # }
            if 'data' in data and isinstance(data['data'], list) and data['data']:
                t = data['data'][0]
                price = float(t['px'])
                qty = float(t['sz'])
                side = 'Buy' if t['side'] == 'buy' else 'Sell'
                timestamp = int(t['ts'])
                trade_time = pd.to_datetime(timestamp, unit='ms', utc=True)
                tz = pytz.timezone('Asia/Seoul')
                trade_time = trade_time.tz_convert(tz)
                new_row = {
                    'Symbol': t['instId'],
                    'Side': side,
                    'Price': price,
                    'Quantity': qty,
                    'Total($)': round(price * qty, 2),
                    'Trade Time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Source': 'OKX'
                }

        if new_row:
            if (set_ticker is None or new_row['Symbol'] in set_ticker) and \
               (minimum_total_dollars is None or new_row['Total($)'] >= minimum_total_dollars):
                socketio.emit('liquidation_update', new_row)

    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

# Update on_close to accept three arguments
def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    print(f"Close status code: {close_status_code}, Close message: {close_msg}")

def on_open_binance(ws):
    print("Binance Connection opened")

def on_open_kraken(ws):
    print("Kraken Connection opened")
    # Subscribe to Kraken trade feed (XBT/USD as example)
    subscribe_msg = {
        "event":"subscribe",
        "pair":["XBT/USD"],
        "subscription":{"name":"trade"}
    }
    ws.send(json.dumps(subscribe_msg))

def on_open_coinbase(ws):
    print("Coinbase Connection opened")
    # Subscribe to Coinbase matches channel
    subscribe_msg = {
        "type": "subscribe",
        "channels": [
            {
                "name": "matches",
                "product_ids": ["BTC-USD"]
            }
        ]
    }
    ws.send(json.dumps(subscribe_msg))

def on_open_okx(ws):
    print("OKX Connection opened")
    # Subscribe to OKX trades channel (BTC-USDT as example)
    subscribe_msg = {
        "op": "subscribe",
        "args": [{"channel": "trades","instId": "BTC-USDT"}]
    }
    ws.send(json.dumps(subscribe_msg))

@app.route("/")
def index():
    return "Liquidation Tracker is Running"

def start_ws():
    # Binance
    binance_ws = websocket.WebSocketApp(binance_socket, on_message=on_message, on_error=on_error, on_close=on_close)
    binance_ws.exchange_name = 'Binance'
    binance_ws.on_open = on_open_binance

    # Kraken
    kraken_ws = websocket.WebSocketApp(kraken_socket, on_message=on_message, on_error=on_error, on_close=on_close)
    kraken_ws.exchange_name = 'Kraken'
    kraken_ws.on_open = on_open_kraken

    # Coinbase
    coinbase_ws = websocket.WebSocketApp(coinbase_socket, on_message=on_message, on_error=on_error, on_close=on_close)
    coinbase_ws.exchange_name = 'Coinbase'
    coinbase_ws.on_open = on_open_coinbase

    # OKX
    okx_ws = websocket.WebSocketApp(okx_socket, on_message=on_message, on_error=on_error, on_close=on_close)
    okx_ws.exchange_name = 'OKX'
    okx_ws.on_open = on_open_okx

    # Start each WebSocket in its own thread
    threading.Thread(target=binance_ws.run_forever, daemon=True).start()
    threading.Thread(target=kraken_ws.run_forever, daemon=True).start()
    threading.Thread(target=coinbase_ws.run_forever, daemon=True).start()
    threading.Thread(target=okx_ws.run_forever, daemon=True).start()

if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_ws)
    ws_thread.daemon = True
    ws_thread.start()
    socketio.run(app, host="0.0.0.0", port=5000)
