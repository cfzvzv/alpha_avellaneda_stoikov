package com.lambda.investing.algorithmic_trading.technical_indicators;

import com.lambda.investing.algorithmic_trading.Algorithm;
import com.lambda.investing.model.candle.Candle;
import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;
import com.tictactec.ta.lib.RetCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Calculator {

	protected static Logger logger = LogManager.getLogger(Calculator.class);

	private static Core CORE = new Core();

	public static double RSICalculate(double[] closePrices, int period) {
		double[] output = new double[closePrices.length];
		MInteger begin = new MInteger();
		MInteger length = new MInteger();

		RetCode retCode = CORE.rsi(0, closePrices.length - 1, closePrices, period, begin, length, output);
		if (retCode.equals(RetCode.Success)) {
			//get the last value
			double outputVal = 0;
			for (int i = 0; i < output.length; i++) {
				if (output[i] == 0) {
					break;
				}
				outputVal = output[i];
			}
			return outputVal;
		} else {
			logger.error("cant calculate RSI -> {}", retCode);
			return Double.NaN;
		}
	}

}
