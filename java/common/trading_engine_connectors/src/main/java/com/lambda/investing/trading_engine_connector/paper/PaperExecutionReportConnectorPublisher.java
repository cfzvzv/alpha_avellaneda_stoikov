package com.lambda.investing.trading_engine_connector.paper;

import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorProvider;
import com.lambda.investing.connector.zero_mq.ZeroMqProvider;
import com.lambda.investing.market_data_connector.MarketDataConnectorPublisher;
import com.lambda.investing.market_data_connector.MarketDataProvider;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;
import com.lambda.investing.model.messaging.Command;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.OrderRequest;
import com.lambda.investing.trading_engine_connector.AbstractPaperExecutionReportConnectorPublisher;
import com.lambda.investing.trading_engine_connector.TradingEngineConnector;
import com.lambda.investing.trading_engine_connector.paper.market.Orderbook;
import com.lambda.investing.trading_engine_connector.paper.market.OrderbookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.lambda.investing.trading_engine_connector.ZeroMqTradingEngineConnector.ALL_ALGORITHMS_SUBSCRIPTION;

public class PaperExecutionReportConnectorPublisher extends AbstractPaperExecutionReportConnectorPublisher {

	private static final boolean NOTIFY_MARKET_TRADES_NOT_EXECUTED = true;
	private Logger logger = LogManager.getLogger(PaperExecutionReportConnectorPublisher.class);

	private MarketDataProvider marketDataProvider;
	private MarketMakerMarketDataExecutionReportListener marketMakerMarketDataExecutionReportListener;
	private ConnectorProvider orderRequestConnectorProvider;
	private ConnectorConfiguration orderRequestConnectorConfiguration;

	private PaperConnectorPublisher paperConnectorMarketDataAndExecutionReportPublisher;
	private PaperConnectorOrderRequestListener paperConnectorOrderRequestListener;

	private List<Instrument> instrumentsList;
	private Map<String, OrderbookManager> orderbookManagerMap;

	public PaperExecutionReportConnectorPublisher(TradingEngineConnector tradingEngineConnector,
			MarketDataProvider marketDataProvider, ConnectorProvider orderRequestConnectorProvider,
			ConnectorConfiguration orderRequestConnectorConfiguration) {
		super(tradingEngineConnector);
		this.marketDataProvider = marketDataProvider;
		this.marketMakerMarketDataExecutionReportListener = new MarketMakerMarketDataExecutionReportListener(this);
		this.orderRequestConnectorProvider = orderRequestConnectorProvider;
		this.orderRequestConnectorConfiguration = orderRequestConnectorConfiguration;

		//listen on this side
		this.paperConnectorOrderRequestListener = new PaperConnectorOrderRequestListener(this,
				this.orderRequestConnectorProvider, this.orderRequestConnectorConfiguration);
	}

	@PostConstruct public void init() {
		//subscribe to data
		this.paperConnectorOrderRequestListener.start();
		this.marketDataProvider.register(this.marketMakerMarketDataExecutionReportListener);
		this.register(ALL_ALGORITHMS_SUBSCRIPTION, this.marketMakerMarketDataExecutionReportListener);
		this.orderRequestConnectorProvider
				.register(this.orderRequestConnectorConfiguration, this.paperConnectorOrderRequestListener);

		logger.info("Starting PaperTrading Engine publishing md/er on {}   listening Orders on {}",
				//MD configuration
				this.paperConnectorMarketDataAndExecutionReportPublisher.getConnectorConfiguration()
						.getConnectionConfiguration(),
				this.orderRequestConnectorConfiguration.getConnectionConfiguration());

		//TODO something more generic on not ZeroMq
		if (this.orderRequestConnectorProvider instanceof ZeroMqProvider) {
			ZeroMqProvider orderRequestConnectorProviderZero = (ZeroMqProvider) this.orderRequestConnectorProvider;
			orderRequestConnectorProviderZero.start(false);//subscribed to all topics on that port
		}
	}

	public void setInstrumentsList(List<Instrument> instrumentsList) {
		this.instrumentsList = instrumentsList;
		//
		logger.info("creating {} orderbooks", instrumentsList.size());
		orderbookManagerMap = new ConcurrentHashMap<>();
		for (Instrument instrument : instrumentsList) {
			Orderbook orderbook = new Orderbook(instrument.getPriceTick());
			OrderbookManager orderbookManager = new OrderbookManager(orderbook, this);
			orderbookManagerMap.put(instrument.getPrimaryKey(), orderbookManager);
		}

	}

	public void setInstrumentsList(Instrument instrument) {
		this.instrumentsList = new ArrayList<>();
		this.instrumentsList.add(instrument);
		setInstrumentsList(this.instrumentsList);

	}

	public void setPaperConnectorMarketDataAndExecutionReportPublisher(
			PaperConnectorPublisher paperConnectorMarketDataAndExecutionReportPublisher) {
		this.paperConnectorMarketDataAndExecutionReportPublisher = paperConnectorMarketDataAndExecutionReportPublisher;
	}

	private String getTopic(String instrumentPk) {
		return instrumentPk;
	}
	private String getTopic(Instrument instrument) {
		return instrument.getPrimaryKey();
	}

	public void notifyCommand(Command command) {
		String topic = "command";
		logger.debug("Notifying command -> \n{}", command.getMessage());
		this.paperConnectorMarketDataAndExecutionReportPublisher.notifyCommand(topic, command);
	}

	/**
	 * Publish the new depth to paper
	 *
	 * @param depth
	 */
	public void notifyDepth(Depth depth) {
		if (!depth.isDepthFilled()) {
			//stop here
			logger.debug("");
		}
		Instrument instrument = Instrument.getInstrument(depth.getInstrument());
		String topic = getTopic(instrument);
		logger.debug("Notifying depth -> \n{}", depth.toString());
		this.paperConnectorMarketDataAndExecutionReportPublisher.notifyDepth(topic, depth);
	}

	/**
	 * Publish the last trade to paper
	 *
	 * @param trade
	 */
	public void notifyTrade(Trade trade) {
		String topic = getTopic(trade.getInstrument());
		logger.debug("Notifying trade -> \n{}", trade.toString());
		this.paperConnectorMarketDataAndExecutionReportPublisher.notifyTrade(topic, trade);

	}

	public void notifyExecutionReport(ExecutionReport executionReport) {
		logger.debug("Notifying execution report -> \n{}", executionReport.toString());
		this.paperConnectorMarketDataAndExecutionReportPublisher.notifyExecutionReport(executionReport);
	}

	public MarketDataConnectorPublisher getMarketDataConnectorPublisher() {
		return paperConnectorMarketDataAndExecutionReportPublisher;
	}

	public boolean orderRequest(OrderRequest orderRequest) {
		//Send orders to the virtual orderbook
		OrderbookManager orderbookManager = orderbookManagerMap.get(orderRequest.getInstrument());
		return orderbookManager.orderRequest(orderRequest);
	}

	public void fillOrderbook(Depth depth) {
		OrderbookManager orderbookManager = orderbookManagerMap.get(depth.getInstrument());
		orderbookManager.refreshMarketMakerDepth(depth);
	}

	public boolean fillMarketTrade(Trade trade) {
		OrderbookManager orderbookManager = orderbookManagerMap.get(trade.getInstrument());
		boolean isNotifiedByExecution = orderbookManager.refreshFillMarketTrade(trade);
		if (NOTIFY_MARKET_TRADES_NOT_EXECUTED && !isNotifiedByExecution) {
			//notify by market
			//			logger.debug("orderbook fill trade ->  {}",trade.toString());
			notifyTrade(trade);
		}
		return true;
	}

}
