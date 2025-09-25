import requests
from config import BINANCE_BASE_URL

def get_symbol_price(symbol="BTCUSDT"):
    """Fetch latest price for a given symbol."""
    url = f"{BINANCE_BASE_URL}/api/v3/ticker/price"
    params = {"symbol": symbol}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_recent_trades(symbol="BTCUSDT", limit=5):
    """Fetch recent trades for a given symbol."""
    url = f"{BINANCE_BASE_URL}/api/v3/trades"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
