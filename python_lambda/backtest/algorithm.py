import os
import glob
import datetime
from typing import Type
import numpy as np
import pandas as pd
from matplotlib import dates
from numpy import genfromtxt, savetxt
import copy
from backtest.algorithm_enum import AlgorithmEnum
from backtest.input_configuration import BacktestConfiguration, TrainInputConfiguration
from backtest.parameter_tuning.ga_configuration import GAConfiguration
from backtest.parameter_tuning.ga_parameter_tuning import GAParameterTuning
from backtest.pnl_utils import get_drawdown
from backtest.train_launcher import TrainLauncher, TrainLauncherController
from configuration import BACKTEST_OUTPUT_PATH


class Algorithm:
    def __init__(self, algorithm_info: str, parameters: dict) -> None:
        super().__init__()
        self.algorithm_info = algorithm_info
        self.parameters = copy.copy(parameters)

    def _is_memory_file(self, file: str, algorithm_name: str):
        return algorithm_name in file and ('qmatrix' in file or 'memoryReplay' in file)

    def _is_model_file(self, file: str, algorithm_name: str):
        return algorithm_name in file and file.endswith('.model')

    def clean_experience(self, output_path):
        # clean it
        for file in os.listdir(output_path):
            if self._is_memory_file(file, algorithm_name=self.algorithm_info):
                print('clean experience qmatrix/memoryReplay %s' % file)
                os.remove(output_path + os.sep + file)

    def clean_model(self, output_path):
        # clean it
        for file in os.listdir(output_path):
            if self._is_model_file(file, algorithm_name=self.algorithm_info):
                print('clean ml model %s' % file)
                os.remove(output_path + os.sep + file)


    def clean_permutation_cache(self, output_path):
        # clean it
        for file in os.listdir(output_path):
            if self.algorithm_info in file and 'permutationStates' in file:
                print('clean permutationStates cache %s' % file)
                os.remove(output_path + os.sep + file)

    def set_parameters(self, parameters: dict):
        for key_param in self.parameters.keys():
            if key_param in parameters.keys():
                self.parameters[key_param] = parameters[key_param]

    def merge_q_matrix(self, backtest_launchers: list) -> list:
        base_path_search = backtest_launchers[0].output_path
        csv_files = glob.glob(base_path_search + os.sep + '*.csv')

        algorithm_names = []
        for backtest_launcher in backtest_launchers:
            algorithm_names.append(backtest_launcher.id)

        csv_files_out = []
        for csv_file in csv_files:
            if 'permutation' in csv_file:
                continue
            for algorithm_name in algorithm_names:
                if self._is_memory_file(csv_file, algorithm_name=algorithm_name):
                    csv_files_out.append(csv_file)
        print(
            'combining %d qmatrix for %d launchers from %s'
            % (len(csv_files_out), len(backtest_launchers), base_path_search)
        )
        output_array = None
        for csv_file_out in csv_files_out:
            my_data = genfromtxt(csv_file_out, delimiter=',')
            print("combining %d rows" % len(my_data))
            if output_array is None:
                output_array = my_data
            else:
                output_array += my_data
        if output_array is None:
            print('cant combine %d files or no data t combine at %s!' % (len(csv_files_out), base_path_search))
            return

        if len(csv_files_out) > 0:
            output_array = output_array / len(csv_files_out)
            if output_array.shape[1]>0:
                output_array = output_array[:, :-1]  # remove last column of nan
            else:
                print ('error combining output_shape in %s'%base_path_search)
        else:
            print('not csv files to merge detected at '+base_path_search)
        # Override it
        print("saving %d rows in %d files" %( len(output_array),len(csv_files_out)))
        for csv_file_out in csv_files_out:
            savetxt(csv_file_out, output_array, delimiter=',', fmt='%.18e')
        return csv_files_out

    def train_model(self, jar_path,train_input_configuration: TrainInputConfiguration):

        train_launcher = TrainLauncher(train_input_configuration=train_input_configuration,jar_path=jar_path ,id='')
        train_launcher_controller = TrainLauncherController(train_launcher)
        train_launcher_controller.run()

    def parameter_tuning(
            self,
            algorithm_enum: AlgorithmEnum,
            start_date: datetime.datetime,
            end_date: datetime,
            instrument_pk: str,
            parameters_base: dict,
            parameters_min: dict,
            parameters_max: dict,
            max_simultaneous: int,
            generations: int,
            ga_configuration: Type[GAConfiguration],
            clean_initial_generation_experience:bool=True
    ) -> (dict, pd.DataFrame):

        backtest_configuration = BacktestConfiguration(
            start_date=start_date, end_date=end_date, instrument_pk=instrument_pk
        )

        ga_parameter_tuning = GAParameterTuning(
            ga_configuration=ga_configuration,
            algorithm=algorithm_enum,
            parameters_base=copy.copy(parameters_base),
            parameters_min=copy.copy(parameters_min),
            parameters_max=copy.copy(parameters_max),
            max_simultaneous=max_simultaneous,
            initial_param_dict_list=[],
        )

        best_param_dict = {}
        for generation in range(generations):
            # clean between generations exp
            if clean_initial_generation_experience:
                print('cleaning outputs/experience on generation %d in path %s' % (generation,BACKTEST_OUTPUT_PATH))
                self.clean_experience(output_path=BACKTEST_OUTPUT_PATH)
                self.clean_permutation_cache(output_path=BACKTEST_OUTPUT_PATH)

            ga_parameter_tuning.run_generation(
                backtest_configuration=backtest_configuration,
                check_unique_population=True,
            )
            best_param_dict = ga_parameter_tuning.get_best_param_dict()

            best_score = ga_parameter_tuning.get_best_score()
        print(
            '%s best score %.4f\nbest_param_dict %s'
            % (algorithm_enum, best_score, best_param_dict)
        )
        return (best_param_dict, ga_parameter_tuning.population_df_out)

    def get_sharpe(self, trade_df: pd.DataFrame) -> float:
        if len(trade_df) == 0:
            return 0.0
        returns = trade_df['returns']
        return returns.mean()/returns.std()


    def get_trade_df(self,raw_trade_pnl_df: pd.DataFrame):
        columns_rn={
            'date':'time',
            'historicalTotalPnl':'total_pnl',
            'historicalRealizedPnl': 'close_pnl',
            'historicalUnrealizedPnl': 'open_pnl',
            'netPosition':'position',

            # #columns=['time', 'risk_aversion', 'windows_tick', 'skew_pct', 'bid', 'ask', 'bid_qty', 'ask_qty',
            #              'imbalance',
            #              'reward'],

            'skewPricePct':'skew_pct',
            'riskAversion': 'risk_aversion',
            'windowTick': 'windows_tick',


        }

        trade_pnl_df=raw_trade_pnl_df.rename(columns=columns_rn)
        trade_pnl_df['time']=pd.to_datetime(trade_pnl_df['timestamp']*1000000)+ pd.DateOffset(hours=1)

        # trade_pnl_df['returns'] = trade_pnl_df['total_pnl'].pct_change().fillna(0)
        # trade_pnl_df['close_returns']=trade_pnl_df['close_pnl'].pct_change().fillna(0)
        # trade_pnl_df['open_returns'] = trade_pnl_df['open_pnl'].pct_change().fillna(0)

        trade_pnl_df['returns'] = trade_pnl_df['total_pnl'].diff().fillna(0)
        trade_pnl_df['close_returns'] = trade_pnl_df['close_pnl'].diff().fillna(0)
        trade_pnl_df['open_returns'] = trade_pnl_df['open_pnl'].diff().fillna(0)

        trade_pnl_df['open_pnl'] = trade_pnl_df['total_pnl']

        trade_pnl_df['returns'] = trade_pnl_df['returns'].replace([np.inf, -np.inf], np.nan)
        trade_pnl_df['close_returns'] = trade_pnl_df['close_returns'].replace([np.inf, -np.inf], np.nan)
        trade_pnl_df['open_returns'] = trade_pnl_df['open_returns'].replace([np.inf, -np.inf], np.nan)
        trade_pnl_df.fillna(method='ffill',inplace=True)

        trade_pnl_df.dropna(axis=0,inplace=True)
        trade_pnl_df['sharpe']=self.get_sharpe(trade_df=trade_pnl_df)
        trade_pnl_df['drawdown']=get_drawdown(trade_pnl_df['total_pnl'])

        return trade_pnl_df

    def plot_params(self,raw_trade_pnl_df: pd.DataFrame , figsize=None,title:str=None):
        import matplotlib.pyplot as plt
        try:
            if figsize is None:
                figsize = (20, 12)

            df=self.get_trade_df(raw_trade_pnl_df=raw_trade_pnl_df)

            print('plotting params from %s to %s' % (df.index[0], df.index[-1]))

            skew_change = True
            windows_change = True
            if df['skew_pct'].fillna(0).diff().fillna(0).sum() == 0:
                skew_change = False

            if df['windows_tick'].fillna(0).diff().fillna(0).sum() == 0:
                windows_change = False

            plt.close()
            subplot_origin = 510
            if skew_change:
                subplot_origin += 100

            if windows_change:
                subplot_origin += 100

            subplot_origin += 1
            plt.subplot(subplot_origin)
            df['risk_aversion'].plot(figsize=figsize)
            plt.legend()
            if title is not None:
                plt.title(title)

            if windows_change:
                subplot_origin += 1
                plt.subplot(subplot_origin)
                df['windows_tick'].plot(figsize=figsize)
                plt.legend()

            if skew_change:
                subplot_origin += 1
                plt.subplot(subplot_origin)
                df['skew_pct'].plot(figsize=figsize)
                plt.legend()

            subplot_origin += 1
            plt.subplot(subplot_origin)
            df['bid'].plot(figsize=figsize)
            df['ask'].plot(figsize=figsize)
            plt.legend()

            subplot_origin += 1
            plt.subplot(subplot_origin)
            df['bid_qty'].plot(figsize=figsize)
            df['ask_qty'].plot(figsize=figsize)
            plt.legend()

            subplot_origin += 1
            plt.subplot(subplot_origin)
            df['imbalance'].plot(figsize=figsize)
            plt.legend()

            subplot_origin += 1
            plt.subplot(subplot_origin)
            df['reward'].plot(figsize=figsize)
            plt.legend()

            plt.gca().xaxis.set_major_locator(dates.MinuteLocator())
            plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))

            plt.show()
        except Exception as e:
            print('Some error plotting params %s' % e)


    def plot_trade_results(self, raw_trade_pnl_df: pd.DataFrame ,title:str=None) -> tuple:
        import warnings
        import matplotlib.pyplot as plt
        trade_pnl_df=self.get_trade_df(raw_trade_pnl_df=raw_trade_pnl_df)
        from matplotlib import MatplotlibDeprecationWarning
        import pandas as pd
        warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)
        warnings.filterwarnings("ignore", category=Warning)

        # plt.style.use('seaborn-white')
        # plt.style.use('seaborn-whitegrid')
        if trade_pnl_df is None:
            print('No trades to plot!')
            return (None, trade_pnl_df)

        if len(trade_pnl_df) == 0:
            print('No trades to plot!')
            return (None, trade_pnl_df)

        # try:
        #
        #     trade_pnl_df['time'] = trade_pnl_df['time'].dt.tz_localize("UTC").dt.tz_convert('Europe/Madrid')
        # except Exception as e:
        #     print('cant move trades time from UTC :%s' % e)

        print('plotting trade_results from %s to %s' % (trade_pnl_df['time'].iloc[0], trade_pnl_df['time'].iloc[-1]))



        # if len(trade_pnl_df) == 0:
        #     print('No trades to plot!')
        #     return (None, trade_pnl_df)
        trade_pnl_df = trade_pnl_df.set_index('time')



        plt.close()
        figsize = (18, 15)  # 1800x1500

        # pnl
        plt.subplot(411)
        trade_pnl_df['close_pnl'].plot(figsize=figsize, style='r', lw=2)
        # trade_pnl_df['realized_pnl'].plot(figsize=figsize, style='r', lw=1)
        trade_pnl_df['open_pnl'].plot(figsize=figsize, style='b')
        plt.legend()
        if title is None:
            plt.title(self.algorithm_info)
        else:
            plt.title(title)
        plt.gca().xaxis.set_major_locator(dates.HourLocator())
        # plt.gca().xaxis.set_major_locator(dates.MinuteLocator())
        plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
        plt.ylabel('returns pct')
        # drawdown
        plt.subplot(412)

        from backtest.pnl_utils import get_drawdown
        dd_open = get_drawdown(trade_pnl_df['open_pnl'])
        dd_close = get_drawdown(trade_pnl_df['close_pnl'])
        dd_close.plot(figsize=figsize, style='--r', lw=2)
        dd_open.plot(figsize=figsize, style='--b')
        plt.legend(['drawdown_open_pnl', 'drawdown_close_pnl'])
        plt.ylabel('losses pct')
        plt.gca().xaxis.set_major_locator(dates.HourLocator())
        plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
        # position sizing
        plt.subplot(413)
        trade_pnl_df['position'].plot(kind='bar', figsize=figsize, style='g')
        # quantity_mean = trade_pnl_df['quantity']*(trade_pnl_df['position'].max())
        # quantity_mean.plot(color='k', figsize=figsize)
        plt.ylabel('position - inventory')
        # plt.legend(['position','quantity_scaled'])
        plt.legend()
        plt.gca().xaxis.set_major_locator(dates.HourLocator())
        plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))

        # returns hist
        plt.subplot(427)
        trade_pnl_df['close_returns'].plot(
            kind='hist', figsize=figsize, stacked=True, style='r'
        )
        trade_pnl_df['returns'].plot(
            kind='hist', figsize=figsize, stacked=True, style='b'
        )
        plt.title('returns - hist')
        plt.ylabel('returns pct')
        plt.legend()
        plt.autoscale(True)
        # plt.tight_layout()

        # add some metrics : sharpe max dd etc
        plt.subplot(428)
        axis = plt.gca()
        sharpe = self.get_sharpe(trade_df=trade_pnl_df)
        if trade_pnl_df['sharpe'][:-1].sum() != 0:
            trade_pnl_df['sharpe'][:-1].plot(figsize=figsize)
            plt.title('rolling sharpe')

        # max_dd_close,max_dd_open,time_max_dd_close,time_max_dd_open
        from backtest.pnl_utils import get_max_drawdowns
        # MDD_start, MDD_end, time_difference, drawdown, UW_dt, UW_duration
        open_time_dd,close_time_dd,td,open_dd, UW_dt, UW_duration  = get_max_drawdowns(trade_pnl_df['open_pnl'])
        open_time_dd, close_time_dd, td, close_dd, UW_dt, UW_duration = get_max_drawdowns(trade_pnl_df['close_pnl'])
        try:
            duration_mins_open = int(open_time_dd.seconds / 60)
        except:
            duration_mins_open = 0
        try:
            duration_mins_close = int(close_time_dd.seconds / 60)
        except:
            duration_mins_close = 0

        textstr = '\n'.join(
            (
                'sharpe=%.4f' % (sharpe,),
                'open max_drawdown pct=%.5f duration_mins=%d'
                % (open_dd / 100, duration_mins_open),
                'close max_drawdown pct=%.5f duration_mins=%d'
                % (close_dd / 100, duration_mins_close),
            )
        )
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
        axis.text(
            0.05,
            0.95,
            textstr,
            transform=axis.transAxes,
            fontsize=16,
            verticalalignment='top',
            bbox=props,
        )

        plt.show()

        return (plt.gcf(), trade_pnl_df)
