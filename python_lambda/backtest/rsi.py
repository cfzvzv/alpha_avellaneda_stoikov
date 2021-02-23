from typing import Type
import pandas as pd
from backtest.algorithm_enum import AlgorithmEnum
from backtest.avellaneda_dqn import AvellanedaDQN, DEFAULT_PARAMETERS
import datetime

from backtest.parameter_tuning.ga_configuration import GAConfiguration
from backtest.score_enum import ScoreEnum

DEFAULT_PARAMETERS_CURRENT = DEFAULT_PARAMETERS
DEFAULT_PARAMETERS_CURRENT['period'] = 14
DEFAULT_PARAMETERS_CURRENT['upperBound'] = 70
DEFAULT_PARAMETERS_CURRENT["lowerBound"] = 30
DEFAULT_PARAMETERS_CURRENT['upperBoundExit'] = 55
DEFAULT_PARAMETERS_CURRENT["lowerBoundExit"] = 45


class RSI(AvellanedaDQN):
    NAME = AlgorithmEnum.rsi

    def __init__(self, algorithm_info: str, parameters: dict = DEFAULT_PARAMETERS_CURRENT):
        super().__init__(
            algorithm_info=self.NAME + "_" + algorithm_info, parameters=parameters
        )


    def train(self, start_date: datetime.datetime, end_date: datetime, instrument_pk: str, iterations: int,
              algos_per_iteration: int, simultaneous_algos: int = 1) -> list:
        # train without directional
        self.parameters['period'] = -1
        self.set_parameters(self.parameters)

        return super().train(start_date, end_date, instrument_pk, iterations, algos_per_iteration, simultaneous_algos)

    def parameter_tuning(
            self,
            start_date: datetime.datetime,
            end_date: datetime,
            instrument_pk: str,
            parameters_min: dict,
            parameters_max: dict,
            max_simultaneous: int,
            generations: int,
            ga_configuration: Type[GAConfiguration],
            parameters_base: dict = DEFAULT_PARAMETERS_CURRENT,
            clean_initial_generation_experience:bool=False,
            algorithm_enum=NAME
    ) -> (dict, pd.DataFrame):

        return super().parameter_tuning(
            start_date=start_date,
            end_date=end_date,
            instrument_pk=instrument_pk,
            parameters_base=parameters_base,
            parameters_min=parameters_min,
            parameters_max=parameters_max,
            max_simultaneous=max_simultaneous,
            generations=generations,
            ga_configuration=ga_configuration,
            clean_initial_generation_experience=clean_initial_generation_experience,
            algorithm_enum=algorithm_enum
        )

if __name__ == '__main__':
    rsi_algorithm = RSI(algorithm_info='test_main_dqn')


    parameters_base_pt = DEFAULT_PARAMETERS
    parameters_base_pt['epoch']=500
    parameters_base_pt['maxBatchSize'] = 5000
    parameters_base_pt['stateColumnsFilter'] = ['ask_price_0',
                                                'ask_price_3',
                                                'ask_price_4',
                                                'ask_price_7',
                                                'ask_price_8',
                                                'ask_qty_0',
                                                'ask_qty_1',
                                                'ask_qty_2',
                                                'ask_qty_4',
                                                'bid_price_0',
                                                'bid_price_3',
                                                'bid_price_4',
                                                'bid_price_7',
                                                'bid_price_8',
                                                'bid_qty_0',
                                                'bid_qty_1',
                                                'bid_qty_4',
                                                'bid_qty_9',
                                                'close_0',
                                                'high_0',
                                                'low_0',
                                                'microprice_0',
                                                'spread_0',
                                                'spread_3',
                                                'spread_4',
                                                'spread_7',
                                                'spread_8']
    parameters_base_pt['changeSide'] = 0

    # best_param_dict, summary_df  = avellaneda_dqn.parameter_tuning(
    #     instrument_pk='btcusdt_binance',
    #     start_date=datetime.datetime(year=2020, day=7, month=12, hour=7),
    #     end_date=datetime.datetime(year=2020, day=7, month=12, hour=9),
    #     parameters_base=parameters_base_pt,
    #     parameters_min={"risk_aversion": 0.1, "window_tick": 3},
    #     parameters_max={"risk_aversion": 0.9, "window_tick": 15},
    #     generations=3,
    #     max_simultaneous=1,
    #     ga_configuration=ga_configuration,
    #
    # )
    rsi_algorithm.set_parameters(parameters=parameters_base_pt)
    print('Starting training')
    # output_train = avellaneda_dqn_directional.train(
    #     instrument_pk='btcusdt_binance',
    #     start_date=datetime.datetime(year=2020, day=8, month=12, hour=10),
    #     end_date=datetime.datetime(year=2020, day=8, month=12, hour=14),
    #     iterations=1,
    #     algos_per_iteration=3,
    #     simultaneous_algos=1,
    # )
    #
    # name_output = avellaneda_dqn_directional.NAME + '_' + avellaneda_dqn_directional.algorithm_info + '_0'
    #
    #
    #
    # backtest_result_train = output_train[0][name_output]
    # # memory_replay_file = r'E:\Usuario\Coding\Python\market_making_fw\python_lambda\output\memoryReplay_AvellanedaDQN_test_main_dqn_0.csv'
    # # memory_replay_df=avellaneda_dqn_directional.get_memory_replay_df(memory_replay_file=memory_replay_file)
    #
    # avellaneda_dqn_directional.plot_trade_results(raw_trade_pnl_df=backtest_result_train, title='train initial')
    #
    # backtest_result_train = output_train[-1][name_output]
    # avellaneda_dqn_directional.plot_trade_results(raw_trade_pnl_df=backtest_result_train, title='train final')

    ga_configuration = GAConfiguration()
    ga_configuration.population = 5

    # best_param_dict, summary_df = rsi_algorithm.parameter_tuning(
    #     instrument_pk='btcusdt_binance',
    #     start_date=datetime.datetime(year=2020, day=7, month=12, hour=7),
    #     end_date=datetime.datetime(year=2020, day=7, month=12, hour=9),
    #     parameters_base=parameters_base_pt,
    #     parameters_min={"period": 5, "upperBound": 55, "lowerBound": 15},
    #     parameters_max={"period": 45, "upperBound": 95, "lowerBound": 45},
    #     generations=5,
    #     max_simultaneous=5,
    #     ga_configuration=ga_configuration,
    # )
    best_param_dict = {'minPrivateState': -1.0,
                       'maxPrivateState': -1.0,
                       'numberDecimalsPrivateState': 3.0,
                       'horizonTicksPrivateState': 5.0,
                       'minMarketState': -1.0,
                       'maxMarketState': -1.0,
                       'numberDecimalsMarketState': 7.0,
                       'horizonTicksMarketState': 10.0,
                       'horizonMinMsTick': 0.0,
                       'minCandleState': -1.0,
                       'maxCandleState': -1.0,
                       'numberDecimalsCandleState': 3.0,
                       'horizonCandlesState': 2.0,
                       'timeHorizonSeconds': 5.0,
                       'epsilon': 0.2,
                       'discountFactor': 0.5,
                       'learningRate': 0.01,
                       'risk_aversion': 0.9,
                       'position_multiplier': 100.0,
                       'window_tick': 10.0,
                       'minutes_change_k': 10.0,
                       'quantity': 0.0001,
                       'k_default': 0.00769,
                       'spread_multiplier': 5.0,
                       'first_hour': 7.0,
                       'last_hour': 19.0,
                       'maxBatchSize': 100000.0,
                       'trainingPredictIterationPeriod': -1.0,
                       'trainingTargetIterationPeriod': -1.0,
                       'epoch': 2000.0,
                       'l1': 0.0,
                       'l2': 0.0,
                       'period': 6.954033316592215,
                       'upperBound': 61.0,
                       'lowerBound': 16.719230021395344,
                       'upperBoundExit': 50.0,
                       'lowerBoundExit': 40.0,
                       'skewPricePctAction': [0.0, 0.05, -0.05, -0.1, 0.1],
                       'riskAversionAction': [0.01, 0.1, 0.2, 0.9],
                       'windowsTickAction': [8],
                       'scoreEnum': 'asymmetric_dampened_pnl',
                       'stateColumnsFilter': ['ask_price_0',
                                              'ask_price_3',
                                              'ask_price_4',
                                              'ask_price_7',
                                              'ask_price_8',
                                              'ask_qty_0',
                                              'ask_qty_1',
                                              'ask_qty_2',
                                              'ask_qty_4',
                                              'bid_price_0',
                                              'bid_price_3',
                                              'bid_price_4',
                                              'bid_price_7',
                                              'bid_price_8',
                                              'bid_qty_0',
                                              'bid_qty_1',
                                              'bid_qty_4',
                                              'bid_qty_9',
                                              'close_0',
                                              'high_0',
                                              'low_0',
                                              'microprice_0',
                                              'spread_0',
                                              'spread_3',
                                              'spread_4',
                                              'spread_7',
                                              'spread_8']}

    print('best parameters as %s' % best_param_dict)
    rsi_algorithm.set_parameters(parameters=best_param_dict)

    print('Starting testing')
    output_test = rsi_algorithm.test(
        instrument_pk='btcusdt_binance',
        start_date=datetime.datetime(year=2020, day=9, month=12, hour=12),
        end_date=datetime.datetime(year=2020, day=9, month=12, hour=18),
    )

    import matplotlib.pyplot as plt

    rsi_algorithm.plot_trade_results(raw_trade_pnl_df=output_test[rsi_algorithm.algorithm_info], title='test')
    plt.show()
    rsi_algorithm.plot_params(raw_trade_pnl_df=output_test[rsi_algorithm.algorithm_info])
    plt.show()
