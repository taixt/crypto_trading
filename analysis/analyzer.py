import matplotlib.pyplot as plt

def plot_price_timeseries(df):
    """Plots price over time from trade data DataFrame."""
    plt.plot(df['time'], df['price'].astype(float))
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.title('Price over Time')
    plt.show()
