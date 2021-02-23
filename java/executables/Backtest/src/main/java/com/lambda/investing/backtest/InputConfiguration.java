package com.lambda.investing.backtest;

import com.lambda.investing.algorithmic_trading.SingleInstrumentAlgorithm;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov.AvellanedaStoikov;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn.*;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_q_learn.AvellanedaStoikovQLearn;
import com.lambda.investing.algorithmic_trading.constant_spread.ConstantSpreadAlgorithm;
import com.lambda.investing.backtest_engine.BacktestConfiguration;
import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Setter @ToString
/***
 * EXAMPLE
 *{
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
 * }
 *
 *
 */ public class InputConfiguration {

	private static int COUNTER_ALGORITHMS = -1;

	private Backtest backtest;
	private Algorithm algorithm;

	public InputConfiguration() {
	}

	public BacktestConfiguration getBacktestConfiguration() throws Exception {
		return backtest.getBacktestConfiguration(algorithm.getAlgorithm());
	}

	@Getter @Setter private class Backtest {

		private String startDate;//20201208
		private String endDate;//20201210

		private String instrument;

		public Backtest() {
		}

		public BacktestConfiguration getBacktestConfiguration(
				com.lambda.investing.algorithmic_trading.Algorithm algorithm) throws Exception {
			Instrument instrumentObject = Instrument.getInstrument(instrument);
			if (instrumentObject == null) {
				throw new Exception("InstrumentPK " + instrument + " not found");
			}
			if (algorithm instanceof SingleInstrumentAlgorithm) {
				((SingleInstrumentAlgorithm) algorithm).setInstrument(instrumentObject);
			}
			algorithm.setPlotStopHistorical(false);
			BacktestConfiguration backtestConfiguration = new BacktestConfiguration();
			backtestConfiguration.setAlgorithm(algorithm);
			backtestConfiguration.setStartTime(startDate);
			backtestConfiguration.setEndTime(endDate);
			backtestConfiguration.setInstrument(instrumentObject);
			backtestConfiguration.setBacktestSource("parquet");
			backtestConfiguration.setSpeed(-1);
			backtestConfiguration.setBacktestExternalConnection("ordinary");

			return backtestConfiguration;
		}
	}

	@Getter @Setter private class Algorithm {

		private String algorithmName;
		private Map<String, Object> parameters;

		/**
		 * Must return the same as in algorithm_enum.py
		 *
		 * @return
		 */
		public com.lambda.investing.algorithmic_trading.Algorithm getAlgorithm() {

			//market making algorithms -> Phd
			if (algorithmName.startsWith("AvellanedaStoikov")) {
				System.out.println("AvellanedaStoikov backtest " + algorithmName);
				return new AvellanedaStoikov(algorithmName, parameters);
			}

			if (algorithmName.startsWith("AvellanedaDQN")) {
				System.out.println("AvellanedaDQN backtest " + algorithmName);
				return new AvellanedaStoikovDQNMarket(algorithmName, parameters);
			}

			if (algorithmName.startsWith("ConstantSpread")) {
				System.out.println("ConstantSpread backtest " + algorithmName);
				return new ConstantSpreadAlgorithm(algorithmName, parameters);
			}

			if (algorithmName.startsWith("AvellanedaQ")) {
				System.out.println("AvellanedaQ backtest " + algorithmName);
				return new AvellanedaStoikovQLearn(algorithmName, parameters);
			}

			///// Directional Algos
			if (algorithmName.startsWith("SMACross")) {
				System.out.println("SMACross backtest " + algorithmName);
				return new SMACross(algorithmName, parameters);
			}

			if (algorithmName.startsWith("RSI")) {
				System.out.println("RSI backtest " + algorithmName);
				return new RSI(algorithmName, parameters);
			}

			System.err.println("algorithm " + algorithmName + " not found!");
			return null;
		}

		public Algorithm() {
		}
	}

}
