from flask import Flask
from flask_socketio import SocketIO, emit
import websocket
import json
import pandas as pd
import pytz

# Flask app setup
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", logger=True, engineio_logger=True)

# Binance WebSocket
socket = 'wss://fstream.binance.com/ws/!forceOrder@arr'

# Filters (set to show all data by default)
minimum_total_dollars = 0
set_ticker = None

# WebSocket message handler
def on_message(ws, message):
    print(f"Raw Data: {message}")  # Debug raw data
    data = json.loads(message)
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
        'Trade Time': trade_time.strftime('%Y-%m-%d %H:%M:%S')
    }

    print(f"Processed Data: {new_row}")  # Debug processed data

    # Emit data to the frontend if it meets the filters
    if (set_ticker is None or new_row['Symbol'] in set_ticker) and \
       (minimum_total_dollars is None or new_row['Total($)'] >= minimum_total_dollars):
        socketio.emit('liquidation_update', new_row)
        print(f"Emitted: {new_row}")  # Debug emitted data

# WebSocket event handlers
def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("Connection closed")

def on_open(ws):
    print("Connection opened")

@app.route("/")
def index():
    return "Liquidation Tracker is Running"

# Start WebSocket connection
def start_ws():
    ws = websocket.WebSocketApp(socket, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":
    import threading
    ws_thread = threading.Thread(target=start_ws)
    ws_thread.daemon = True
    ws_thread.start()
    socketio.run(app, host="0.0.0.0", port=5000)
