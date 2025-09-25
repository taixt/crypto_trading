import threading
import time
from collections import deque
from datetime import timezone
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation

from data_logger import DataLogger
from services.binance_streamer import BinanceStreamer
from services.binance_orderbook import BinanceOrderBookStreamer
from services.binance_ticker import BinanceTickerStreamer
from services.bitfinex_streamer import BitfinexStreamer
from services.bitfinex_orderbook import BitfinexOrderBookStreamer
from services.bitfinex_ticker import BitfinexTickerStreamer

from analysis.indicators import compute_vwap, spread, liquidity_imbalance

# -------------------------------
# Initialize Logger
# -------------------------------
logger = DataLogger("data")

# -------------------------------
# Initialize Streamers
# -------------------------------
binance_trades = BinanceStreamer("btcusdt", window_seconds=120, logger=logger)
binance_ob = BinanceOrderBookStreamer("btcusdt", top_n=5, logger=logger)
binance_ticker = BinanceTickerStreamer("btcusdt", logger=logger)

bitfinex_trades = BitfinexStreamer("tBTCUSD", window_seconds=120, logger=logger)
bitfinex_ob = BitfinexOrderBookStreamer("tBTCUSD", top_n=5, logger=logger)
bitfinex_ticker = BitfinexTickerStreamer("tBTCUSD", logger=logger)

# Start WebSockets
binance_trades.start_websocket()
binance_ob.start()
binance_ticker.start()

bitfinex_trades.start_websocket()
bitfinex_ob.start()
bitfinex_ticker.start()

# -------------------------------
# Rolling Data Storage for Plotting
# -------------------------------
max_len = 300
b_prices, b_vols, b_times = deque(maxlen=max_len), deque(maxlen=max_len), deque(maxlen=max_len)
f_prices, f_vols, f_times = deque(maxlen=max_len), deque(maxlen=max_len), deque(maxlen=max_len)

# -------------------------------
# Update Function (Background)
# -------------------------------
def update_data():
    while True:
        # Binance Trades
        if binance_trades.timestamps:
            b_times.append(binance_trades.timestamps[-1].to_pydatetime())
            b_prices.append(binance_trades.prices[-1])
            b_vols.append(binance_trades.volumes[-1])

        # Bitfinex Trades
        if bitfinex_trades.timestamps:
            f_times.append(bitfinex_trades.timestamps[-1].to_pydatetime())
            f_prices.append(bitfinex_trades.prices[-1])
            f_vols.append(bitfinex_trades.volumes[-1])

        time.sleep(0.5)

threading.Thread(target=update_data, daemon=True).start()

# -------------------------------
# Live Dashboard
# -------------------------------
fig, axs = plt.subplots(5, 1, figsize=(14, 12), sharex=True)
plt.subplots_adjust(hspace=0.3)

def animate(frame):
    for ax in axs:
        ax.clear()

    # ---- Price Plot ----
    if b_times:
        axs[0].plot(b_times, b_prices, color="blue", label="Binance")
    if f_times:
        axs[0].plot(f_times, f_prices, color="orange", label="Bitfinex")
    axs[0].set_ylabel("Price (USD)")
    axs[0].legend()
    axs[0].set_title("Live BTC Price")

    # ---- VWAP ----
    if b_prices and b_vols:
        axs[1].plot(b_times, [compute_vwap(list(b_prices)[:i+1], list(b_vols)[:i+1]) for i in range(len(b_prices))], color="blue", label="Binance VWAP")
    if f_prices and f_vols:
        axs[1].plot(f_times, [compute_vwap(list(f_prices)[:i+1], list(f_vols)[:i+1]) for i in range(len(f_prices))], color="orange", label="Bitfinex VWAP")
    axs[1].set_ylabel("VWAP")
    axs[1].legend()

    # ---- Spread (best ask - best bid) ----
    # Take top level from order books if available
    if binance_ob.bids and binance_ob.asks and bitfinex_ob.bids and bitfinex_ob.asks:
        b_top_bid, b_top_ask = binance_ob.bids[0][0], binance_ob.asks[0][0]
        f_top_bid, f_top_ask = bitfinex_ob.bids[0][0], bitfinex_ob.asks[0][0]
        axs[2].bar(["Binance", "Bitfinex"], [b_top_ask - b_top_bid, f_top_ask - f_top_bid], color=["blue","orange"])
        axs[2].set_ylabel("Spread (USD)")
        axs[2].set_title("Current Spread")

    # ---- Liquidity Imbalance ----
    if binance_ob.bids and binance_ob.asks:
        b_liq = liquidity_imbalance(binance_ob.bids, binance_ob.asks)
    else:
        b_liq = 0
    if bitfinex_ob.bids and bitfinex_ob.asks:
        f_liq = liquidity_imbalance(bitfinex_ob.bids, bitfinex_ob.asks)
    else:
        f_liq = 0
    axs[3].bar(["Binance", "Bitfinex"], [b_liq, f_liq], color=["blue","orange"])
    axs[3].set_ylabel("Liquidity Imbalance")
    axs[3].set_title("Top 5 Bids vs Asks")

    # ---- Trade Volume ----
    if b_times:
        axs[4].bar(b_times, b_vols, width=0.0005, alpha=0.5, color="blue", label="Binance")
    if f_times:
        axs[4].bar(f_times, f_vols, width=0.0005, alpha=0.5, color="orange", label="Bitfinex")
    axs[4].set_ylabel("Volume")
    axs[4].legend()
    axs[4].set_title("Trade Volume")

    # Format x-axis
    axs[4].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S', tz=timezone.utc))
    plt.setp(axs[4].xaxis.get_majorticklabels(), rotation=30)

ani = FuncAnimation(fig, animate, interval=1000)
plt.show()
