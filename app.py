from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import websocket
import json
import pandas as pd
import pytz
import threading
import os

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", logger=True, engineio_logger=True)

# WebSocket URLs for exchanges
binance_socket = 'wss://fstream.binance.com/ws/!forceOrder@arr'
kraken_socket = 'wss://ws.kraken.com'
coinbase_socket = 'wss://ws-feed.pro.coinbase.com'
okx_socket = 'wss://ws.okx.com:8443/ws/v5/public'

# Filters
minimum_total_dollars = 500
set_ticker = None

def on_message(ws, message):
    source = getattr(ws, 'exchange_name', 'Unknown')
    try:
        print(f"Raw message from {source}: {message}")  # Debugging raw data
        data = json.loads(message)
        new_row = None

        if source == 'Binance':
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
            if isinstance(data, list) and len(data) > 3 and data[2] == 'trade':
                trades = data[1]
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
                    break

        elif source == 'Coinbase':
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
                print(f"Emitted: {new_row}")  # Debugging emitted data

    except Exception as e:
        print(f"Error processing message from {source}: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    print(f"Close status code: {close_status_code}, Close message: {close_msg}")

def on_open_binance(ws):
    print("Binance Connection opened")

def on_open_kraken(ws):
    print("Kraken Connection opened")
    subscribe_msg = {
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(subscribe_msg))

def on_open_coinbase(ws):
    print("Coinbase Connection opened")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]
    }
    ws.send(json.dumps(subscribe_msg))

def on_open_okx(ws):
    print("OKX Connection opened")
    subscribe_msg = {
        "op": "subscribe",
        "args": [{"channel": "trades", "instId": "BTC-USDT"}]
    }
    ws.send(json.dumps(subscribe_msg))

@app.route("/")
def index():
    return jsonify({"status": "Backend is running!"})

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

    threading.Thread(target=binance_ws.run_forever, daemon=True).start()
    threading.Thread(target=kraken_ws.run_forever, daemon=True).start()
    threading.Thread(target=coinbase_ws.run_forever, daemon=True).start()
    threading.Thread(target=okx_ws.run_forever, daemon=True).start()

if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_ws)
    ws_thread.daemon = True
    ws_thread.start()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
