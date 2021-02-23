import datetime


def generate_candle_time(df, freq='1Min'):
    return df.resample(freq).ohlc()


def generate_candle_volume(df, freq=5000):
    # TODO
    return df.resample(freq).ohlc()
