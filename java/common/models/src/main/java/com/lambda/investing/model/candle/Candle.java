package com.lambda.investing.model.candle;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter public class Candle {

	private CandleType candleType;
	private double high, low, open, close;

	public Candle(CandleType candleType, double open, double high, double low, double close) {
		this.candleType = candleType;
		this.high = high;
		this.low = low;
		this.open = open;
		this.close = close;
	}

}
