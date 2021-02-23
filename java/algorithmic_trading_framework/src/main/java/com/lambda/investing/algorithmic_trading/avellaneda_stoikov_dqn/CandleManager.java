package com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn;

import com.lambda.investing.algorithmic_trading.reinforcement_learning.state.AbstractState;
import com.lambda.investing.algorithmic_trading.reinforcement_learning.state.StateManager;
import com.lambda.investing.model.candle.Candle;
import com.lambda.investing.model.candle.CandleType;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;
import org.apache.curator.shaded.com.google.common.collect.EvictingQueue;

import java.util.Date;
import java.util.Queue;

public class CandleManager {

	private static double DEFAULT_MAX_PRICE = -9E9;
	private static double DEFAULT_MIN_PRICE = 9E9;

	private long lastTimestampTradeCandle = 0;
	private double maxPrice = DEFAULT_MAX_PRICE;
	private double minPrice = DEFAULT_MIN_PRICE;
	private double openPrice = -1.;

	private StateManager stateManager;

	public CandleManager(StateManager stateManager) {
		this.stateManager = stateManager;

	}

	public boolean onDepthUpdate(Depth depth) {
		return true;
	}

	public boolean onTradeUpdate(Trade trade) {
		Date date = new Date(trade.getTimestamp());
		if (date.getMinutes() != lastTimestampTradeCandle) {
			if (openPrice == -1) {
				//first candle
				openPrice = trade.getPrice();
				maxPrice = trade.getPrice();
				minPrice = trade.getPrice();

				lastTimestampTradeCandle = date.getMinutes();
				return true;
			}

			maxPrice = Math.max(maxPrice, trade.getPrice());
			minPrice = Math.min(minPrice, trade.getPrice());
			assert maxPrice >= minPrice;
			assert maxPrice >= openPrice;
			assert maxPrice >= trade.getPrice();
			assert minPrice <= openPrice;
			assert minPrice <= trade.getPrice();

			Candle candle = new Candle(CandleType.time_1_min, openPrice, maxPrice, minPrice, trade.getPrice());

			stateManager.onUpdateCandle(candle);
			lastTimestampTradeCandle = date.getMinutes();
			openPrice = trade.getPrice();
			maxPrice = trade.getPrice();
			minPrice = trade.getPrice();
		} else {
			maxPrice = Math.max(maxPrice, trade.getPrice());
			minPrice = Math.min(minPrice, trade.getPrice());

		}
		return true;
	}

}
