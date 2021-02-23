import datetime
import pandas as pd
import pyarrow.parquet as pq
import os
from glob import glob
import numpy as np
from configuration import PARQUET_PATH_DB
from database.candle_generation import *


# root_db_path = rf'\\nas\home\lambda_data'


# D:\javif\Coding\cryptotradingdesk\data\type=trade\instrument=btceur_binance\date=20201204
def get_microprice(depth_df):
    volumes = depth_df['askQuantity0']+ depth_df['bidQuantity0']
    return depth_df['askPrice0']*(depth_df['askQuantity0']/volumes)+depth_df['bidPrice0']*(depth_df['bidQuantity0']/volumes)

def get_imbalance(depth_df,max_depth:int=5):
    total_ask_vol=None
    total_bid_vol=None
    for market_horizon_i in range(max_depth):
        if total_ask_vol is None:
            total_ask_vol = depth_df['askQuantity%d' % market_horizon_i]
        else:
            total_ask_vol += depth_df['askQuantity%d'%market_horizon_i]

        if total_bid_vol is None:
            total_bid_vol = depth_df['bidQuantity%d' % market_horizon_i]
        else:
            total_bid_vol += depth_df['bidQuantity%d' % market_horizon_i]
    imbalance = (total_bid_vol-total_ask_vol)/(total_bid_vol+total_ask_vol)
    return imbalance


class TickDB:
    default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
    default_end_date = datetime.datetime.today()

    def __init__(self, root_db_path: str = PARQUET_PATH_DB) -> None:
        self.base_path = root_db_path

        self.date_str_format = '%Y%m%d'

    def get_all_data(self, instrument_pk: str,
                          start_date: datetime.datetime = default_start_date,
                          end_date: datetime.datetime = default_end_date)-> pd.DataFrame:
        depth_df = self.get_depth(instrument_pk=instrument_pk, start_date=start_date, end_date=end_date)
        trades_df = self.get_trades(instrument_pk=instrument_pk, start_date=start_date, end_date=end_date)
        candles_df = self.get_candles_time(instrument_pk=instrument_pk, start_date=start_date, end_date=end_date)

        depth_df_2 = depth_df.reset_index()
        depth_df_2['type'] = 'depth'

        trades_df_2 = trades_df.reset_index()
        trades_df_2['type'] = 'trade'

        candles_df.columns = ['_'.join(col).strip() for col in candles_df.columns.values]
        candles_df_2 = candles_df.reset_index()
        candles_df_2['type'] = 'candle'


        backtest_data = pd.concat([depth_df_2, trades_df_2,candles_df_2])

        backtest_data.set_index(keys='date', inplace=True)
        backtest_data.sort_index(inplace=True)
        backtest_data.fillna(method='ffill', inplace=True)
        # backtest_data.dropna(inplace=True)
        return backtest_data

    # def get_all_trades(self, instrument_pk: str):
    #     type_data = 'trade'
    #     path_trades=rf"{self.base_path}\type={type_data}\instrument={instrument_pk}"
    #     return pd.read_parquet(path_trades)
    #
    # def get_all_depth(self, instrument_pk: str):
    #     type_data = 'depth'
    #     return pd.read_parquet(rf"{self.base_path}\type={type_data}\instrument={instrument_pk}")

    def get_all_instruments(self, type_str: str = 'trade'):
        source_path = rf"{self.base_path}\type={type_str}"
        all_folders = glob(source_path + "/*")
        instruments = []
        for folder in all_folders:
            instrument = folder.split("instrument=")[-1]
            instruments.append(instrument)
        return instruments

    def get_all_dates(self, type_str: str, instrument_pk: str):
        source_path = rf"{self.base_path}\type={type_str}\instrument={instrument_pk}"
        all_folders = glob(source_path + "/*")
        dates = []
        for folder in all_folders:
            date_str = folder.split("date=")[-1]
            date = datetime.datetime.strptime(date_str, self.date_str_format)
            dates.append(date)
        return dates




    def get_depth(
            self,
            instrument_pk: str,
            start_date: datetime.datetime = default_start_date,
            end_date: datetime.datetime = default_end_date,
    ):
        start_date_str = start_date.strftime(self.date_str_format)
        end_date_str = end_date.strftime(self.date_str_format)
        type_data = 'depth'
        source_path = rf"{self.base_path}\type={type_data}\instrument={instrument_pk}"
        print(
            "downloading %s %s from %s to %s"
            % (instrument_pk, type_data, start_date_str, end_date_str)
        )
        dataset = pq.ParquetDataset(
            source_path,
            filters=[('date', '>=', start_date_str), ('date', '<=', end_date_str)],
        )
        table = dataset.read()
        df = table.to_pandas()

        df['date'] = pd.to_datetime(df.index * 1000000)
        df.dropna(inplace=True)
        df.set_index('date', inplace=True)
        return df

    def get_trades(
            self,
            instrument_pk: str,
            start_date: datetime.datetime = default_start_date,
            end_date: datetime.datetime = default_end_date,
    ):
        start_date_str = start_date.strftime(self.date_str_format)
        end_date_str = end_date.strftime(self.date_str_format)
        type_data = 'trade'
        source_path = rf"{self.base_path}\type={type_data}\instrument={instrument_pk}"
        print(
            "downloading %s %s from %s to %s"
            % (instrument_pk, type_data, start_date_str, end_date_str)
        )
        dataset = pq.ParquetDataset(
            source_path,
            filters=[('date', '>=', start_date_str), ('date', '<=', end_date_str)],
        )
        table = dataset.read()
        df = table.to_pandas()
        df['date'] = pd.to_datetime(df.index * 1000000)
        df.dropna(inplace=True)
        df.set_index('date', inplace=True)
        return df

    def get_candles_time(
            self,
            instrument_pk: str,
            start_date: datetime.datetime = default_start_date,
            end_date: datetime.datetime = default_end_date,
            freq='1 Min',
    ):

        start_date_str = start_date.strftime(self.date_str_format)
        end_date_str = end_date.strftime(self.date_str_format)
        type_data = 'candle_time_%s' % (freq.replace(" ", ""))
        source_path = rf"{self.base_path}\type={type_data}\instrument={instrument_pk}"
        try:
            dataset = pq.ParquetDataset(
                source_path,
                filters=[('date', '>=', start_date_str), ('date', '<=', end_date_str)],
            )
            table = dataset.read()
            df = table.to_pandas()
        except:
            trades_df = self.get_trades(
                instrument_pk=instrument_pk, start_date=start_date, end_date=end_date
            )
            df = generate_candle_time(df=trades_df, freq=freq)
            # TODO save it to cache?

        return df


if __name__ == '__main__':
    tick = TickDB()
    instrument_pk = 'btcusdt_binance'

    # instruments = tick.get_all_instruments()
    # dates = tick.get_all_dates(type_str='depth', instrument_pk=instrument_pk)
    # # trades_df_all = tick.get_all_trades(instrument_pk=LambdaInstrument.btcusdt_binance)
    # trades_df = tick.get_trades(instrument_pk=instrument_pk)
    # depth_df = tick.get_depth(instrument_pk=instrument_pk)
    all = tick.get_all_data(instrument_pk=instrument_pk,start_date=datetime.datetime(year=2020, day=7, month=12),end_date=datetime.datetime(year=2020, day=7, month=12))
