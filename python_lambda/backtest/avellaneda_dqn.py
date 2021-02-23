import datetime
from typing import Type
import copy
import shutil
import time

from numpy import genfromtxt, savetxt

from backtest.algorithm import Algorithm
from backtest.algorithm_enum import AlgorithmEnum
from backtest.backtest_launcher import BacktestLauncher, BacktestLauncherController
from backtest.input_configuration import (
    BacktestConfiguration,
    AlgorithmConfiguration,
    InputConfiguration,
    JAR_PATH,
    TrainInputConfiguration)
import glob
import os
import pandas as pd
from backtest.parameter_tuning.ga_configuration import GAConfiguration
from backtest.score_enum import ScoreEnum

DEFAULT_PARAMETERS = {
    # DQN parameters
    "maxBatchSize": 1000,
    "trainingPredictIterationPeriod": -1,  # train only at the end,offline
    "trainingTargetIterationPeriod": -1,  # train at the end,offline
    "epoch": 50,

    # Q
    "skewPricePctAction": [0.0],
    "riskAversionAction": [0.9, 0.5],
    "windowsTickAction": [5, 10],
    "minPrivateState": (-0.01),
    "maxPrivateState": (-0.01),
    "minMarketState": -1,
    "maxMarketState": -1,
    "minCandleState": -1,
    "maxCandleState": -1,

    "numberDecimalsPrivateState": (-1),
    "numberDecimalsMarketState": (-1),
    "numberDecimalsCandleState": (-1),

    "horizonTicksPrivateState": (5),
    "horizonTicksMarketState": (10),
    "horizonCandlesState": (1),

    "horizonMinMsTick": (0),
    "scoreEnum": ScoreEnum.total_pnl,
    "timeHorizonSeconds": (5),
    "epsilon": (0.2),
    "discountFactor": 0.95,
    "learningRate": 0.25,

    # Avellaneda default
    "risk_aversion": (0.9),
    "position_multiplier": (100),
    "window_tick": (10),
    "minutes_change_k": (10),
    "quantity": (0.0001),
    "k_default": (0.00769),
    "spread_multiplier": (5.0),
    "first_hour": (7),
    "last_hour": (19),
    "l1":0.,
    "l2":0.,
    # [inventory_2, inventory_1, inventory_0,
    # score_2, score_1, score_0,
    # bid_price_4, bid_price_3, bid_price_2, bid_price_1, bid_price_0,
    # ask_price_4, ask_price_3, ask_price_2, ask_price_1, ask_price_0,
    # bid_qty_4, bid_qty_3, bid_qty_2, bid_qty_1, bid_qty_0,
    # ask_qty_4,ask_qty_3, ask_qty_2, ask_qty_1, ask_qty_0,
    # spread_4, spread_3, spread_2, spread_1, spread_0,
    # midprice_4, midprice_3, midprice_2, midprice_1, midprice_0,
    # imbalance_4, imbalance_3, imbalance_2, imbalance_1, imbalance_0,
    # microprice_4, microprice_3, microprice_2, microprice_1, microprice_0,
    # last_close_price_4, last_close_price_3, last_close_price_2, last_close_price_1, last_close_price_0,
    # last_close_qty_4, last_close_qty_3, last_close_qty_2, last_close_qty_1, last_close_qty_0,
    # open_4, open_3, open_2, open_1, open_0,
    # high_4, high_3, high_2, high_1, high_0,
    # low_4, low_3, low_2, low_1, low_0,
    # close_4, close_3, close_2, close_1, close_0,
    # ma, std, max, min]
    "stateColumnsFilter": []
}

PRIVATE_COLUMNS = 2
MARKET_COLUMNS = 10
CANDLE_COLUMNS = 4
CANDLE_INDICATOR_COLUMNS = 4


class AvellanedaDQN(Algorithm):
    NAME = AlgorithmEnum.avellaneda_dqn

    def __init__(self, algorithm_info: str, parameters: dict = DEFAULT_PARAMETERS):
        super().__init__(
            algorithm_info=algorithm_info, parameters=parameters
        )
        self.is_filtered_states=False
        if 'stateColumnsFilter' in parameters.keys() and parameters['stateColumnsFilter'] is not None and len(parameters['stateColumnsFilter'])>0:
            self.is_filtered_states=True



    def get_parameters(self, explore_prob: float) -> dict:
        parameters = copy.copy(self.parameters)
        parameters['epsilon'] = explore_prob
        return parameters

    def _get_default_state_columns(self):
        MARKET_MIDPRICE_RELATIVE=True

        private_states=[]
        market__depth_states=[]
        candle_states=[]
        market__trade_states=[]
        private_horizon_ticks=self.parameters['horizonTicksPrivateState']
        market_horizon_ticks=self.parameters['horizonTicksMarketState']
        candle_horizon=self.parameters['horizonCandlesState']

        for private_state_horizon in range(private_horizon_ticks-1,-1,-1):
            private_states.append('inventory_%d'%private_state_horizon)

        for private_state_horizon in range(private_horizon_ticks-1,-1,-1):
            private_states.append('score_%d'%private_state_horizon)

        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('bid_price_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('ask_price_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('bid_qty_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('ask_qty_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('spread_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('midprice_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('imbalance_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__depth_states.append('microprice_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__trade_states.append('last_close_price_%d'%market_state_horizon)
        for market_state_horizon in range(market_horizon_ticks-1,-1,-1):
            market__trade_states.append('last_close_qty_%d'%market_state_horizon)


        if not MARKET_MIDPRICE_RELATIVE:
            for candle_state_horizon in range(candle_horizon-1,-1,-1):
                candle_states.append('open_%d'%candle_state_horizon)
        for candle_state_horizon in range(candle_horizon-1,-1,-1):
            candle_states.append('high_%d'%candle_state_horizon)
        for candle_state_horizon in range(candle_horizon-1,-1,-1):
            candle_states.append('low_%d'%candle_state_horizon)
        for candle_state_horizon in range(candle_horizon-1,-1,-1):
            candle_states.append('close_%d'%candle_state_horizon)

        candle_states.append('ma')
        candle_states.append('std')
        candle_states.append('max')
        candle_states.append('min')

        columns_states=private_states+market__depth_states+market__trade_states+candle_states
        return columns_states

    def _get_action_columns(self):
        skew_actions=self.parameters['skewPricePctAction']
        risk_aversion_actions=self.parameters['riskAversionAction']
        windows_actions=self.parameters['windowsTickAction']
        actions=[]
        counter=0
        for action in skew_actions:
            for risk_aversion in risk_aversion_actions:
                for windows_action in windows_actions:
                    actions.append('action_%d_reward'%counter)
                    counter+=1
        return actions


    def get_memory_replay_df(self,memory_replay_file:str,state_columns:list=None)->pd.DataFrame:
        if state_columns is None:
            if self.is_filtered_states:
                #add private
                private_horizon_ticks=self.parameters['horizonTicksPrivateState']
                state_columns_temp=[]
                for private_state_horizon in range(private_horizon_ticks-1,-1,-1):
                    state_columns_temp.append('inventory_%d'%private_state_horizon)
                for private_state_horizon in range(private_horizon_ticks-1,-1,-1):
                    state_columns_temp.append('score_%d'%private_state_horizon)
                state_columns_temp += self.parameters['stateColumnsFilter']
                state_columns=state_columns_temp
                # sort_ordered=self._get_default_state_columns()

                #Sort columns in the right order
                # state_columns=[]
                # for state_column in sort_ordered:
                #     if state_column in state_columns_temp:
                #         state_columns.append(state_column)

            else:
                state_columns=self._get_default_state_columns()

        action_columns=self._get_action_columns()
        next_state_actions=copy.copy(state_columns)

        all_columns=state_columns+action_columns+next_state_actions

        ## read memory
        if not os.path.exists(memory_replay_file):
            print('file not found %s'%memory_replay_file)
            return None
        my_data = genfromtxt(memory_replay_file, delimiter=',')
        assert my_data.shape[1]==len(all_columns)
        output = pd.DataFrame(my_data,columns=all_columns)
        return output



    def train(
            self,
            start_date: datetime.datetime,
            end_date: datetime,
            instrument_pk: str,
            iterations: int,
            algos_per_iteration: int,
            simultaneous_algos: int = 1,
    ) -> list:

        backtest_configuration = BacktestConfiguration(
            start_date=start_date, end_date=end_date, instrument_pk=instrument_pk
        )
        momentum_nesterov = self.parameters['discountFactor']
        learning_rate=self.parameters['learningRate']
        number_epochs=self.parameters['epoch']
        l1 = self.parameters['l1']
        l2 = self.parameters['l2']

        output_list = []
        for iteration in range(iterations):
            print("------------------------")
            print("Training iteration %d/%d"%(iteration,iterations-1))
            print("------------------------")

            backtest_launchers = []
            for algorithm_number in range(algos_per_iteration):
                parameters = self.get_parameters(
                    explore_prob=1 - iteration / iterations
                )
                algorithm_name = '%s_%s_%d' % (
                    self.NAME,
                    self.algorithm_info,
                    algorithm_number,
                )
                print('training on algorithm %s' % algorithm_name)
                algorithm_configurationQ = AlgorithmConfiguration(
                    algorithm_name=algorithm_name, parameters=parameters
                )
                input_configuration = InputConfiguration(
                    backtest_configuration=backtest_configuration,
                    algorithm_configuration=algorithm_configurationQ,
                )

                backtest_launcher = BacktestLauncher(
                    input_configuration=input_configuration,
                    id=algorithm_name,
                    jar_path=JAR_PATH,
                )
                backtest_launchers.append(backtest_launcher)

            if iteration == 0 and os.path.isdir(backtest_launchers[0].output_path):
                # clean it
                print('cleaning experience on training  path %s' % backtest_launchers[0].output_path)
                self.clean_experience(output_path=backtest_launchers[0].output_path)
                print('cleaning models on training  path %s' % backtest_launchers[0].output_path)
                self.clean_model(output_path=backtest_launchers[0].output_path)

            # in case number of states/actions changes
            self.clean_permutation_cache(output_path=backtest_launchers[0].output_path)

            # Launch it
            backtest_controller = BacktestLauncherController(
                backtest_launchers=backtest_launchers,
                max_simultaneous=simultaneous_algos,
            )
            output_dict = backtest_controller.run()
            output_list.append(output_dict)
            # Combine experience
            memory_files = self.merge_q_matrix(backtest_launchers=backtest_launchers)

            predict_models = []
            target_models = []
            for memory_file in memory_files:
                predict_models.append(memory_file.replace('memoryReplay', 'predict_model').replace('.csv', '.model'))
                target_models.append(memory_file.replace('memoryReplay', 'target_model').replace('.csv', '.model'))

            # train nn
            memory_file = memory_files[0]
            predict_model = predict_models[0]
            # target_model = target_models[0]

            state_columns = self.get_number_of_state_columns(self.parameters)
            action_columns = self.get_number_of_action_columns(self.parameters)

            train_input_configuration = TrainInputConfiguration(memory_path=memory_file,
                                                                output_model_path=predict_model,
                                                                state_columns=state_columns,
                                                                action_columns=action_columns,
                                                                number_epochs=number_epochs,
                                                                learning_rate=learning_rate,
                                                                l1=l1,
                                                                l2=l2,
                                                                momentum_nesterov=momentum_nesterov,
                                                                )
            print(f'training {predict_model} on {memory_file}')
            self.train_model(jar_path=JAR_PATH, train_input_configuration=train_input_configuration)
            # copy to all  for next iteration have a trained nn
            if os.path.exists(predict_model):
                for predict_model_it in predict_models:
                    if predict_model == predict_model_it:
                        continue
                    print(f'copying predict model {predict_model} in {predict_model_it}')
                    shutil.copy(predict_model, predict_model_it)
                for target_model_it in target_models:
                    print(f'copying target model {predict_model} in {target_model_it}')
                    shutil.copy(predict_model, target_model_it)

        return output_list

    def test(
            self,
            start_date: datetime.datetime,
            end_date: datetime,
            instrument_pk: str,
            explore_prob: float = 0.2,
            algorithm_number: int = 0,
            clean_experience: bool = False,
    ) -> dict:
        backtest_configuration = BacktestConfiguration(
            start_date=start_date, end_date=end_date, instrument_pk=instrument_pk
        )
        parameters = self.get_parameters(explore_prob=explore_prob)

        algorithm_name = '%s_%s_%d' % (self.NAME, self.algorithm_info, algorithm_number)
        print('testing on algorithm %s'%algorithm_name)
        algorithm_configurationQ = AlgorithmConfiguration(
            algorithm_name=algorithm_name, parameters=parameters
        )
        input_configuration = InputConfiguration(
            backtest_configuration=backtest_configuration,
            algorithm_configuration=algorithm_configurationQ,
        )

        backtest_launcher = BacktestLauncher(
            input_configuration=input_configuration,
            id=algorithm_name,
            jar_path=JAR_PATH,
        )

        if clean_experience:
            print('cleaning experience on test on path %s' % backtest_launcher.output_path)
            self.clean_experience(output_path=backtest_launcher.output_path)
            print('cleaning models on training  path %s' % backtest_launcher.output_path)
            self.clean_model(output_path=backtest_launcher.output_path)

        backtest_controller = BacktestLauncherController(
            backtest_launchers=[backtest_launcher], max_simultaneous=1
        )
        output_dict = backtest_controller.run()

        if (output_dict is None or len(output_dict)==0):
            print("not output generated in java! something was wrong")

        output_dict[self.algorithm_info] = output_dict[algorithm_name]
        del output_dict[algorithm_name]

        return output_dict

    def get_number_of_state_columns(self, parameters: dict) -> int:
        state_columns=[]
        if 'stateColumnsFilter' in list(parameters.keys()):
            state_columns = parameters['stateColumnsFilter']
            for state_str in copy.copy(state_columns):
                # remove private not filtered! to add it later
                if 'score' in state_str or 'inventory' in state_str:
                    del state_columns[state_str]


        if state_columns is None or len(state_columns) == 0:
            number_state_columns = parameters['horizonTicksPrivateState'] * PRIVATE_COLUMNS + parameters[
                'horizonTicksMarketState'] * MARKET_COLUMNS + parameters[
                                       'horizonCandlesState'] * CANDLE_COLUMNS + CANDLE_INDICATOR_COLUMNS
        else:
            #add private columns
            number_state_columns = len(state_columns)
            number_state_columns+=(parameters['horizonTicksPrivateState'] * PRIVATE_COLUMNS)

        return number_state_columns

    def get_number_of_action_columns(self, parameters: dict) -> int:
        number_of_lists = 0
        list_of_lists=[]
        for parameter_key in parameters.keys():
            value = parameters[parameter_key]
            if isinstance(value, list) and parameter_key != 'stateColumnsFilter':
                number_of_lists += 1
                list_of_lists.append(value)
        assert number_of_lists==3
        number_of_actions=0
        for first_list in list_of_lists[0]:
            for second_list in list_of_lists[1]:
                for third_list in list_of_lists[2]:
                    number_of_actions+=1
        return number_of_actions

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
            parameters_base: dict = DEFAULT_PARAMETERS,
            clean_initial_generation_experience:bool=True,
            algorithm_enum=AlgorithmEnum.avellaneda_dqn
    ) -> (dict, pd.DataFrame):

        return super().parameter_tuning(
            algorithm_enum=algorithm_enum,
            start_date=start_date,
            end_date=end_date,
            instrument_pk=instrument_pk,
            parameters_base=parameters_base,
            parameters_min=parameters_min,
            parameters_max=parameters_max,
            max_simultaneous=max_simultaneous,
            generations=generations,
            ga_configuration=ga_configuration,
            clean_initial_generation_experience=clean_initial_generation_experience
        )

    def merge_q_matrix(self, backtest_launchers: list) -> list:
        import numpy as np
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
        csv_files_out=list(set(csv_files_out))
        print(
            'combining %d memory_replay for %d launchers from %s'
            % (len(csv_files_out), len(backtest_launchers), base_path_search)
        )
        assert len(csv_files_out) == len(backtest_launchers)
        output_array = None
        for csv_file_out in csv_files_out:
            try:
                my_data = genfromtxt(csv_file_out, delimiter=',')
                print("combining %d rows from %s" % (len(my_data),csv_file_out))
                if len(my_data)==0 or len(my_data[0][:])==0:
                    print('has no valid data ,ignore %s'%csv_file_out)
                    continue

            except Exception as e:
                print('error loading memory %s-> skip it  %s' % (csv_file_out, e.args))
                continue
            my_data = my_data[:, :-1]  # remove last column of nan
            if output_array is None:
                output_array = my_data
            else:
                output_array = np.append(output_array.T, my_data.T, axis=1).T
        if output_array is None:
            print('cant combine %d files or no data t combine at %s!' % (len(csv_files_out), base_path_search))
            return
        df = pd.DataFrame(output_array)

        number_state_columns = self.get_number_of_state_columns(self.parameters)
        df_state_columns = list(df.columns)[:number_state_columns]

        df = df.groupby(df_state_columns).mean()

        max_batch_size = self.parameters['maxBatchSize']
        print("saving %d rows in %d files" % (len(output_array), len(csv_files_out)))
        # Override it randomize
        for csv_file_out in csv_files_out:
            if len(df) > max_batch_size:
                df = df.sample(max_batch_size)
            output_array = df.reset_index().values

            savetxt(csv_file_out, output_array, delimiter=',', fmt='%.18e')
        return csv_files_out

    def set_parameters(self, parameters: dict):
        super().set_parameters(parameters)
        if 'stateColumnsFilter' in parameters.keys() and parameters['stateColumnsFilter'] is not None and len(parameters['stateColumnsFilter'])>0:
            self.is_filtered_states=True



if __name__ == '__main__':
    avellaneda_dqn = AvellanedaDQN(algorithm_info='test_main_dqn')


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
    avellaneda_dqn.set_parameters(parameters=parameters_base_pt)
    print('Starting training')
    output_train = avellaneda_dqn.train(
        instrument_pk='btcusdt_binance',
        start_date=datetime.datetime(year=2020, day=8, month=12, hour=10),
        end_date=datetime.datetime(year=2020, day=8, month=12, hour=14),
        iterations=1,
        algos_per_iteration=3,
        simultaneous_algos=1,
    )

    name_output = avellaneda_dqn.NAME + '_' + avellaneda_dqn.algorithm_info + '_0'



    backtest_result_train = output_train[0][name_output]
    memory_replay_file = r'E:\Usuario\Coding\Python\market_making_fw\python_lambda\output\memoryReplay_AvellanedaDQN_test_main_dqn_0.csv'
    memory_replay_df=avellaneda_dqn.get_memory_replay_df(memory_replay_file=memory_replay_file)

    avellaneda_dqn.plot_trade_results(raw_trade_pnl_df=backtest_result_train,title='train initial')

    backtest_result_train = output_train[-1][name_output]
    avellaneda_dqn.plot_trade_results(raw_trade_pnl_df=backtest_result_train,title='train final')



    print('Starting testing')
    output_test = avellaneda_dqn.test(
        instrument_pk='btcusdt_binance',
        start_date=datetime.datetime(year=2020, day=9, month=12, hour=12),
        end_date=datetime.datetime(year=2020, day=9, month=12, hour=14),
    )

    import matplotlib.pyplot as plt
    avellaneda_dqn.plot_trade_results(raw_trade_pnl_df=output_test[avellaneda_dqn.algorithm_info],title='test')
    plt.show()
    avellaneda_dqn.plot_params(raw_trade_pnl_df=output_test[avellaneda_dqn.algorithm_info])
    plt.show()
