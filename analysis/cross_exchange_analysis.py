import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Load data
bin_df = pd.read_csv("../data/trades_binance_btcusdt.csv")
bit_df = pd.read_csv("../data/trades_bitfinex_tBTCUSD.csv")

# Convert to UTC datetime and drop invalid rows
for df in (bin_df, bit_df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df.dropna(subset=["timestamp"], inplace=True)
    df.sort_values("timestamp", inplace=True)

# Resample to 1-second bars (last trade of each second)
bin_sec = bin_df.set_index("timestamp").resample("1S").last()
bit_sec = bit_df.set_index("timestamp").resample("1S").last()

# Combine into a single DataFrame for proper alignment
combined = pd.DataFrame({
    "binance_price": bin_sec["price"],
    "bitfinex_price": bit_sec["price"],
})

# Drop rows where either price is missing (no trades in that second)
combined.dropna(inplace=True)

# Compute spread
combined["spread"] = combined["bitfinex_price"] - combined["binance_price"]

# Plot spread
fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(combined.index, combined["spread"], label="Spread (Bitfinex - Binance)")
ax.axhline(0, color="gray", linestyle="--", linewidth=1)

ax.set_xlabel("Time (UTC)")
ax.set_ylabel("Spread (USD)")
ax.set_title("Cross-Exchange BTC/USDT Spread")
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
fig.autofmt_xdate()

plt.show()

# Summary statistics
print("Spread Summary:")
print(combined["spread"].describe())

# Optional: Show correlation
corr = combined["binance_price"].corr(combined["bitfinex_price"])
print(f"\nPrice Correlation: {corr:.4f}")
