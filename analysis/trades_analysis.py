import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def compute_vwap(df):
    return (df['price'] * df['volume']).cumsum() / df['volume'].cumsum()

# Load CSVs
bin_df = pd.read_csv("../data/trades_binance_btcusdt.csv")
bit_df = pd.read_csv("../data/trades_bitfinex_tBTCUSD.csv")

# Convert timestamp column to UTC datetime safely
for df in (bin_df, bit_df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df.dropna(subset=["timestamp"], inplace=True)

bin_df = bin_df.sort_values("timestamp").reset_index(drop=True)
bit_df = bit_df.sort_values("timestamp").reset_index(drop=True)

# Compute VWAP
bin_df['vwap'] = compute_vwap(bin_df)
bit_df['vwap'] = compute_vwap(bit_df)

# Plot
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(bin_df['timestamp'], bin_df['price'], label='Binance Price', alpha=0.6)
ax.plot(bin_df['timestamp'], bin_df['vwap'], label='Binance VWAP', linewidth=2)
ax.plot(bit_df['timestamp'], bit_df['price'], label='Bitfinex Price', alpha=0.6)
ax.plot(bit_df['timestamp'], bit_df['vwap'], label='Bitfinex VWAP', linewidth=2)

ax.set_xlabel("Time (UTC)")
ax.set_ylabel("Price (USD)")
ax.set_title("Price vs VWAP")
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
fig.autofmt_xdate()

plt.show()

