import pandas as pd
import matplotlib.pyplot as plt

# Load Binance order book
ob_bin = pd.read_csv("../data/orderbook_binance_btcusdt.csv", parse_dates=["timestamp"])
ob_bit = pd.read_csv("../data/orderbook_bitfinex_tBTCUSD.csv", parse_dates=["timestamp"])

# Compute Spread
ob_bin['spread'] = ob_bin['ask1'] - ob_bin['bid1']
ob_bit['spread'] = ob_bit['ask1'] - ob_bit['bid1']

# Compute Liquidity Imbalance (top 5 levels)
def liquidity_imbalance(df):
    bids = df[['bid1','bid2','bid3','bid4','bid5']].sum(axis=1)
    asks = df[['ask1','ask2','ask3','ask4','ask5']].sum(axis=1)
    return (bids - asks) / (bids + asks)

ob_bin['imbalance'] = liquidity_imbalance(ob_bin)
ob_bit['imbalance'] = liquidity_imbalance(ob_bit)

# Plot Spread
plt.figure(figsize=(14,4))
plt.plot(ob_bin['timestamp'], ob_bin['spread'], label='Binance Spread')
plt.plot(ob_bit['timestamp'], ob_bit['spread'], label='Bitfinex Spread')
plt.xlabel("Time")
plt.ylabel("Spread (USD)")
plt.title("Order Book Spread")
plt.legend()
plt.show()

# Plot Liquidity Imbalance
plt.figure(figsize=(14,4))
plt.plot(ob_bin['timestamp'], ob_bin['imbalance'], label='Binance Imbalance')
plt.plot(ob_bit['timestamp'], ob_bit['imbalance'], label='Bitfinex Imbalance')
plt.xlabel("Time")
plt.ylabel("Liquidity Imbalance")
plt.title("Order Book Liquidity Imbalance")
plt.legend()
plt.show()
