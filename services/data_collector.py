import pandas as pd
from services.binance_api import get_recent_trades

def collect_trades(symbol="BTCUSDT", limit=10):
    trades = get_recent_trades(symbol, limit)
    df = pd.DataFrame(trades)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df
