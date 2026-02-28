import pandas as pd
import ta


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds technical indicator columns to a DataFrame with OHLCV data.

    Expected input columns: Close, High, Low, Open, Volume
    Returns a new DataFrame with all original columns plus indicator columns,
    with rows containing null values dropped.
    """
    df = df.copy()

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    open_ = df["Open"]

    # RSI (14-period)
    df["RSI_14"] = ta.momentum.RSIIndicator(close=close, window=14).rsi()

    # Simple Moving Averages
    df["SMA_20"] = ta.trend.SMAIndicator(close=close, window=20).sma_indicator()
    df["SMA_50"] = ta.trend.SMAIndicator(close=close, window=50).sma_indicator()
    df["SMA_200"] = ta.trend.SMAIndicator(close=close, window=200).sma_indicator()

    # Exponential Moving Average (9-period)
    df["EMA_9"] = ta.trend.EMAIndicator(close=close, window=9).ema_indicator()

    # Average True Range (14-period)
    df["ATR_14"] = ta.volatility.AverageTrueRange(
        high=high, low=low, close=close, window=14
    ).average_true_range()

    # Bollinger Bands (20-period, 2 standard deviations)
    bb = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
    df["BB_upper"] = bb.bollinger_hband()
    df["BB_middle"] = bb.bollinger_mavg()
    df["BB_lower"] = bb.bollinger_lband()

    # Swing highs and swing lows (each candle vs its immediate neighbors)
    df["swing_high"] = (
        (df["High"] > df["High"].shift(1)) & (df["High"] > df["High"].shift(-1))
    )
    df["swing_low"] = (
        (df["Low"] < df["Low"].shift(1)) & (df["Low"] < df["Low"].shift(-1))
    )

    # Candle metrics
    df["body_size"] = (close - open_).abs()
    df["candle_range"] = high - low
    df["body_ratio"] = df["body_size"] / df["candle_range"].replace(0, float("nan"))

    # Momentum (10-period percentage change)
    df["momentum_10"] = close.pct_change(periods=10) * 100

    # Drop rows with any null values introduced by indicator lookback periods
    df = df.dropna()

    return df


if __name__ == "__main__":
    import os

    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "historical", "EUR_USD.csv"
    )

    raw = pd.read_csv(csv_path, index_col="Datetime", parse_dates=True)
    result = add_indicators(raw)

    print("Shape:", result.shape)
    print("Columns:")
    for col in result.columns:
        print(" ", col)
