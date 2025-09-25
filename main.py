from services.data_collector import collect_trades
from analysis.analyzer import plot_price_timeseries
from services.price_streamer import PriceStreamer

def main():
    choice = input("Choose mode: (1) Historical Fetch  (2) Live Stream: ").strip()

    if choice == "1":
        df = collect_trades("BTCUSDT", 50)
        print(df.head())
        plot_price_timeseries(df)

    elif choice == "2":
        print("Starting live price chart. Close the window or press Ctrl+C to stop.")
        streamer = PriceStreamer("BTCUSDT")
        streamer.start()

    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()