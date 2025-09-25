import time
from data_logger import DataLogger
from services.binance_streamer import BinanceStreamer
from services.binance_orderbook import BinanceOrderBookStreamer
from services.binance_ticker import BinanceTickerStreamer
from services.bitfinex_streamer import BitfinexStreamer
from services.bitfinex_orderbook import BitfinexOrderBookStreamer
from services.bitfinex_ticker import BitfinexTickerStreamer

# -------------------------------
# Initialize Logger
# -------------------------------
logger = DataLogger("data")

# -------------------------------
# Initialize Streamers
# -------------------------------
# Binance
# binance_trades = BinanceStreamer("btcusdt", "binance", logger)
# binance_ob = BinanceOrderBookStreamer("btcusdt", "binance", top_n=5, logger=logger)
# binance_ticker = BinanceTickerStreamer("btcusdt", "binance", logger=logger)

# Bitfinex
bitfinex_trades = BitfinexStreamer("tBTCUSD", "bitfinex", logger)
bitfinex_ob = BitfinexOrderBookStreamer("tBTCUSD", "bitfinex", top_n=5, logger=logger)
bitfinex_ticker = BitfinexTickerStreamer("tBTCUSD", "bitfinex", logger=logger)

# -------------------------------
# Start WebSockets
# -------------------------------
# binance_trades.start_websocket()
# binance_ob.start()
# binance_ticker.start()

bitfinex_trades.start_websocket()
bitfinex_ob.start()
bitfinex_ticker.start()

# -------------------------------
# Keep Running
# -------------------------------
try:
    print("Data collection running... Press Ctrl+C to stop.")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Data collection stopped.")
