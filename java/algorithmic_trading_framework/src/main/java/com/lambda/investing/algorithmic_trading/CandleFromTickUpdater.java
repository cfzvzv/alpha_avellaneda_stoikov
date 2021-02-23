package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.model.candle.Candle;
import com.lambda.investing.model.candle.CandleType;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;

import java.util.Date;

public class CandleFromTickUpdater {

	private static double DEFAULT_MAX_PRICE = -9E9;
	private static double DEFAULT_MIN_PRICE = 9E9;
	
	
	private Date lastTimestampMinuteTradeCandle = null;
	private double maxPriceMinuteTrade = DEFAULT_MAX_PRICE;
	private double minPriceMinuteTrade = DEFAULT_MIN_PRICE;
	private double openPriceMinuteTrade = -1.;
	

	private Date lastTimestampHourTradeCandle = null;
	private double maxPriceHourTrade = DEFAULT_MAX_PRICE;
	private double minPriceHourTrade = DEFAULT_MIN_PRICE;
	private double openPriceHourTrade = -1.;


	private Date lastTimestampMinuteMidCandle = null;
	private double maxPriceMinuteMid = DEFAULT_MAX_PRICE;
	private double minPriceMinuteMid = DEFAULT_MIN_PRICE;
	private double openPriceMinuteMid = -1.;
	
	private Date lastTimestampDepthCandle = null;
	
	private Algorithm algorithmToNotify;

	public CandleFromTickUpdater(Algorithm algorithmToNotify) {
		this.algorithmToNotify = algorithmToNotify;
	}


	
	private void generateTradeMinuteCandle(Trade trade){
		Date date = new Date(trade.getTimestamp());
		if (openPriceMinuteTrade == -1) {
			//first candle
			openPriceMinuteTrade = trade.getPrice();
			maxPriceMinuteTrade = trade.getPrice();
			minPriceMinuteTrade = trade.getPrice();
			lastTimestampMinuteTradeCandle = date;
			return;
		}

		maxPriceMinuteTrade = Math.max(maxPriceMinuteTrade, trade.getPrice());
		minPriceMinuteTrade = Math.min(minPriceMinuteTrade, trade.getPrice());
		assert maxPriceMinuteTrade >= minPriceMinuteTrade;
		assert maxPriceMinuteTrade >= openPriceMinuteTrade;
		assert maxPriceMinuteTrade >= trade.getPrice();
		assert minPriceMinuteTrade <= openPriceMinuteTrade;
		assert minPriceMinuteTrade <= trade.getPrice();

		Candle candle = new Candle(CandleType.time_1_min, openPriceMinuteTrade, maxPriceMinuteTrade, minPriceMinuteTrade, trade.getPrice());

		algorithmToNotify.onUpdateCandle(candle);
		lastTimestampMinuteTradeCandle = date;
		openPriceMinuteTrade = trade.getPrice();
		maxPriceMinuteTrade = trade.getPrice();
		minPriceMinuteTrade = trade.getPrice();
	}

	private void generateMidMinuteCandle(Depth depth){
		Date date = new Date(depth.getTimestamp());
		if (openPriceMinuteMid == -1) {
			//first candle
			openPriceMinuteMid = depth.getMidPrice();
			maxPriceMinuteMid = depth.getMidPrice();
			minPriceMinuteMid = depth.getMidPrice();
			lastTimestampMinuteMidCandle = date;
			return;
		}

		maxPriceMinuteMid = Math.max(maxPriceMinuteMid, depth.getMidPrice());
		minPriceMinuteMid = Math.min(minPriceMinuteMid, depth.getMidPrice());
		assert maxPriceMinuteMid >= minPriceMinuteMid;
		assert maxPriceMinuteMid >= openPriceMinuteMid;
		assert maxPriceMinuteMid >= depth.getMidPrice();
		assert minPriceMinuteMid <= openPriceMinuteMid;
		assert minPriceMinuteMid <= depth.getMidPrice();

		Candle candle = new Candle(CandleType.mid_time_1_min, openPriceMinuteMid, maxPriceMinuteMid, minPriceMinuteMid, depth.getMidPrice());

		algorithmToNotify.onUpdateCandle(candle);
		lastTimestampMinuteMidCandle = date;
		openPriceMinuteMid = depth.getMidPrice();
		maxPriceMinuteMid = depth.getMidPrice();
		minPriceMinuteMid = depth.getMidPrice();
	}
	private void generateTradeHourCandle(Trade trade){
		Date date = new Date(trade.getTimestamp());
		if (openPriceHourTrade == -1) {
			//first candle
			openPriceHourTrade = trade.getPrice();
			maxPriceHourTrade = trade.getPrice();
			minPriceHourTrade = trade.getPrice();
			lastTimestampHourTradeCandle = date;
			return;
		}

		maxPriceHourTrade = Math.max(maxPriceHourTrade, trade.getPrice());
		minPriceHourTrade = Math.min(minPriceHourTrade, trade.getPrice());
		assert maxPriceHourTrade >= minPriceHourTrade;
		assert maxPriceHourTrade >= openPriceHourTrade;
		assert maxPriceHourTrade >= trade.getPrice();
		assert minPriceHourTrade <= openPriceHourTrade;
		assert minPriceHourTrade <= trade.getPrice();

		Candle candle = new Candle(CandleType.time_1_hour, openPriceHourTrade, maxPriceHourTrade, minPriceHourTrade, trade.getPrice());

		algorithmToNotify.onUpdateCandle(candle);
		lastTimestampHourTradeCandle = date;
		openPriceHourTrade = trade.getPrice();
		maxPriceHourTrade = trade.getPrice();
		minPriceHourTrade = trade.getPrice();
	}



	public boolean onDepthUpdate(Depth depth) {
		//minute candle
		Date date = new Date(depth.getTimestamp());
		if (lastTimestampMinuteMidCandle==null || date.getMinutes() != lastTimestampMinuteMidCandle.getMinutes()) {
			generateMidMinuteCandle(depth);
		}

		return true;
	}
	
	
	public boolean onTradeUpdate(Trade trade) {
		Date date = new Date(trade.getTimestamp());
		
		//minute candle
		if (lastTimestampMinuteTradeCandle==null || date.getMinutes() != lastTimestampMinuteTradeCandle.getMinutes()) {
			generateTradeMinuteCandle(trade);
		}

		//hour candle
		if (lastTimestampHourTradeCandle==null || date.getHours() != lastTimestampHourTradeCandle.getHours()) {
			generateTradeHourCandle(trade);
		}
		
		return true;
	}

}
