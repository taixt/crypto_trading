import pandas as pd
import numpy as np

def compute_vwap(prices, volumes):
    df = pd.DataFrame({"price": prices, "volume": volumes})
    return (df["price"] * df["volume"]).sum() / df["volume"].sum() if df["volume"].sum() > 0 else 0

def liquidity_imbalance(bids, asks):
    bid_total = sum(size for _, size in bids[:5])
    ask_total = sum(size for _, size in asks[:5])
    return (bid_total - ask_total) / (bid_total + ask_total) if (bid_total + ask_total) != 0 else 0

def spread(bids, asks):
    return asks[0][0] - bids[0][0] if bids and asks else 0

def trade_size_stats(amounts):
    amounts = np.array(amounts)
    return {"mean": amounts.mean(), "median": np.median(amounts), "max": amounts.max(), "min": amounts.min()} if len(amounts) > 0 else {}
