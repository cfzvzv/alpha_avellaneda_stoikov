import datetime
import json
import uuid
import os

from configuration import BACKTEST_JAR_PATH

'''
 * 	"backtest": {
 * 		"startDate": "20201208",
 * 		"endDate": "20201208",
 * 		"instrument": "btcusdt_binance"
 *        },
 * 	"algorithm": {
 * 		"algorithmName": "AvellanedaStoikov",
 * 		"parameters": {
 * 			"risk_aversion": "0.9",
 * 			"position_multiplier": "100",
 * 			"window_tick": "100",
 * 			"minutes_change_k": "10",
 * 			"quantity": "0.0001",
 * 			"k_default": "0.00769",
 * 			"spread_multiplier": "5.0",
 * 			"first_hour": "7",
 * 			"last_hour": "19"
 *        }
 *    }
 *
 * }'''

format_date = '%Y%m%d'
format_date_hour = '%Y%m%d %H:%M:%S'
import pandas as pd

JAR_PATH = BACKTEST_JAR_PATH


class BacktestConfiguration:
    def __init__(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        instrument_pk: str,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.instrument_pk = instrument_pk

    def get_json(self) -> str:
        output_dict = {}
        if (self.start_date==self.end_date):
            #day format
            print('deprecated format date for backtest! include hours')
            output_dict['startDate'] = self.start_date.strftime(format_date)
            output_dict['endDate'] = self.end_date.strftime(format_date)
        else:
            #hour format
            output_dict['startDate'] = self.start_date.strftime(format_date_hour)
            output_dict['endDate'] = self.end_date.strftime(format_date_hour)
        output_dict['instrument'] = self.instrument_pk
        json_object = json.dumps(output_dict)
        return json_object

def get_parameters_string(parameters: dict):
    parameters_out = {}
    for parameter_key in parameters.keys():
        value = parameters[parameter_key]
        if isinstance(value, list):
            value = ''.join([str(elem) + ',' for elem in value])[:-1]
        else:
            value = str(value)
        parameters_out[parameter_key] = value
    return parameters_out


class AlgorithmConfiguration:
    def __init__(self, algorithm_name: str, parameters: dict):
        self.algorithm_name = algorithm_name
        self.parameters = parameters

    def get_json(self) -> str:
        output_dict = {}
        output_dict['algorithmName'] = self.algorithm_name
        output_dict['parameters'] = get_parameters_string(self.parameters)

        json_object = json.dumps(output_dict)
        return json_object


class InputConfiguration:
    def __init__(
        self,
        backtest_configuration: BacktestConfiguration,
        algorithm_configuration: AlgorithmConfiguration,
    ):
        self.backtest_configuration = backtest_configuration
        self.algorithm_configuration = algorithm_configuration

    def get_json(self) -> str:
        json_backtest__object = rf'"backtest":{self.backtest_configuration.get_json()}'
        json_algo__object = rf'"algorithm":{self.algorithm_configuration.get_json()}'
        return '{' + json_backtest__object + ',\n' + json_algo__object + "}"

    def get_filename(self):

        return (
            self.algorithm_configuration.algorithm_name
            + '_'
            + str(uuid.uuid1())
            + '.json'
        )

class TrainInputConfiguration:
    '''
    {
  "memoryPath": "memoryReplay_avellanedaDQNsample_btcusdt_binance.csv",
  "outputModelPath":"memoryReplay_avellanedaDQNsample_btcusdt_binance_csv.model",
  "actionColumns":4,
  "stateColumns":6,
  "nEpoch":100,
  "l2":0.0001,
  "l1":0.0,
  "learningRate":0.25,
  "momentumNesterov":0.5,
  "trainingStats":0
}

    '''
    def __init__(self,memory_path:str,output_model_path:str,action_columns:int,state_columns:int,number_epochs:int,l2:float=0.0001,l1:float=0.0,learning_rate:float=0.25,momentum_nesterov:float=0.5,training_stats:int=0):
        self.memory_path=memory_path
        if not os.path.exists(self.memory_path):
            raise Exception('memory_path not found %s '%self.memory_path)
        # else:
        #     self.memory_path=os.path.abspath(self.memory_path)
        self.output_model_path=output_model_path
        self.action_columns=action_columns
        self.state_columns=state_columns
        self.number_epochs=number_epochs
        self.l2=l2
        self.l1=l1
        self.learning_rate=learning_rate
        self.momentum_nesterov=momentum_nesterov
        self.training_stats=training_stats
        self.check_memory_inputs()

    def check_memory_inputs(self):
        df_memory = pd.read_csv(self.memory_path)
        columns_memory = len(df_memory.columns)
        assert columns_memory==self.action_columns+2*self.state_columns


    def get_json(self) -> str:
        output_dict = {}
        output_dict['memoryPath'] = self.memory_path
        output_dict['outputModelPath'] = self.output_model_path

        output_dict['actionColumns'] = self.action_columns
        output_dict['stateColumns'] = self.state_columns
        output_dict['nEpoch'] = self.number_epochs
        output_dict['l2'] = self.l2
        output_dict['l1'] = self.l1
        output_dict['learningRate'] = self.learning_rate
        output_dict['momentumNesterov'] = self.momentum_nesterov
        output_dict['trainingStats']=self.training_stats

        json_object = json.dumps(output_dict)
        return json_object

    def get_filename(self):

        return (
            'train_input'
            + '_'
            + str(uuid.uuid1())
            + '.json'
        )

