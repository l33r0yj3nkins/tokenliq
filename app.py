from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import websocket
import json
import threading
import os
from dotenv import load_dotenv
import tweepy
from PIL import Image
import time

# Load environment variables
load_dotenv()

# Flask and SocketIO setup
app = Flask(__name__, template_folder="templates")
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent", logger=True, engineio_logger=True)

# WebSocket URLs for exchanges
sockets = {
    'Binance': 'wss://fstream.binance.com/ws/!forceOrder@arr',
    'Kraken': 'wss://ws.kraken.com',
    'Coinbase': 'wss://ws-feed.pro.coinbase.com',
    'OKX': 'wss://ws.okx.com:8443/ws/v5/public',
    'Gate.io': 'wss://ws.gate.io/v4',
    'MEXC': 'wss://wbs.mexc.com/ws'
}

# Twitter API authentication
auth = tweepy.OAuthHandler(
    os.getenv("TWITTER_API_KEY"),
    os.getenv("TWITTER_API_SECRET")
)
auth.set_access_token(
    os.getenv("TWITTER_ACCESS_TOKEN"),
    os.getenv("TWITTER_ACCESS_SECRET")
)
twitter_api = tweepy.API(auth)

def send_tweet(data):
    """Send liquidation data to Twitter."""
    try:
        tweet_text = (
            f"🚨 High Liquidation Alert 🚨\n"
            f"Exchange: {data['Source']}\n"
            f"Symbol: {data['Symbol']}\n"
            f"Side: {data['Side']}\n"
            f"Price: ${data['Price']:.2f}\n"
            f"Quantity: {data['Quantity']:.2f}\n"
            f"Total: ${data['Total($)']:.2f}\n"
            f"Time: {data['Trade Time']}"
        )

        image_path = "images/liqed.png"
        if os.path.exists(image_path):
            media = twitter_api.media_upload(image_path)
            twitter_api.update_status(status=tweet_text, media_ids=[media.media_id])
            print(f"Tweeted: {tweet_text}")
        else:
            twitter_api.update_status(status=tweet_text)
            print(f"Tweeted without image: {tweet_text}")
    except Exception as e:
        print(f"Error sending tweet: {e}")

def process_binance_message(message):
    """Process Binance liquidation data."""
    data = json.loads(message)
    order = data['o']
    return {
        'Symbol': order['s'],
        'Side': order['S'],
        'Price': float(order['p']),
        'Quantity': float(order['q']),
        'Total($)': round(float(order['p']) * float(order['q']), 2),
        'Trade Time': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
        'Source': 'Binance'
    }

def on_message(ws, message):
    """Handle incoming WebSocket messages."""
    source = ws.exchange_name
    print(f"Raw message from {source}: {message}")
    try:
        if source == 'Binance':
            data = process_binance_message(message)
            if data['Total($)'] > 49999:
                send_tweet(data)
                socketio.emit('liquidation_update', data)
        else:
            print(f"Processing not implemented for {source}")
    except Exception as e:
        print(f"Error processing message from {source}: {e}")

def on_error(ws, error):
    print(f"Error in {ws.exchange_name}: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed for {ws.exchange_name}. Code: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    """Subscribe to liquidation data for exchanges requiring subscription."""
    print(f"Connection opened for {ws.exchange_name}")
    if ws.exchange_name == 'Kraken':
        ws.send(json.dumps({
            "event": "subscribe",
            "pair": ["BTC/USD", "ETH/USD"],
            "subscription": {"name": "trade"}
        }))
    elif ws.exchange_name == 'Coinbase':
        ws.send(json.dumps({
            "type": "subscribe",
            "channels": [{"name": "matches", "product_ids": ["BTC-USD", "ETH-USD"]}]
        }))
    elif ws.exchange_name == 'OKX':
        ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": "BTC-USDT"}]
        }))

def start_ws():
    """Start WebSocket connections for all exchanges."""
    threads = []
    for name, url in sockets.items():
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.exchange_name = name
        ws.on_open = on_open
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    threading.Thread(target=start_ws, daemon=True).start()
    port = int(os.getenv("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
