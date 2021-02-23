package com.lambda.investing.market_data_connector;

import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

public class MarketDataPainter implements MarketDataListener {

	MarketDataProvider marketDataProvider;

	protected Logger logger = LogManager.getLogger(MarketDataPainter.class);

	public MarketDataPainter() {

	}

	public void setMarketDataProvider(MarketDataProvider marketDataProvider) {
		this.marketDataProvider = marketDataProvider;
	}

	@PostConstruct
	public void init(){
		logger.info("init MarketDataPainter");
		//todo why is null?
		marketDataProvider.register(this);
	}

	@Override public void onDepthUpdate(Depth depth) {
		//		logger.info("received {}",depth.toString());

	}

	@Override public void onTradeUpdate(Trade trade) {

		//		logger.info("received {}",trade.toString());
	}

	@Override public void onCommandUpdate(String command) {

	}
}
