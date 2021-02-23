package com.lambda.investing.backtest_engine.ordinary;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lambda.investing.backtest_engine.AbstractBacktest;
import com.lambda.investing.backtest_engine.BacktestConfiguration;
import com.lambda.investing.backtest_engine.BacktestSource;
import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorProvider;
import com.lambda.investing.connector.ConnectorPublisher;
import com.lambda.investing.connector.ThreadUtils;
import com.lambda.investing.connector.ordinary.OrdinaryConnectorConfiguration;
import com.lambda.investing.connector.ordinary.OrdinaryConnectorPublisherProvider;
import com.lambda.investing.market_data_connector.MarketDataConnectorPublisher;
import com.lambda.investing.market_data_connector.MarketDataConnectorPublisherListener;
import com.lambda.investing.market_data_connector.MarketDataProvider;
import com.lambda.investing.market_data_connector.csv_file_reader.CSVFileConfiguration;
import com.lambda.investing.market_data_connector.csv_file_reader.CSVMarketDataConnectorPublisher;
import com.lambda.investing.market_data_connector.ordinary.OrdinaryMarketDataProvider;
import com.lambda.investing.market_data_connector.parquet_file_reader.ParquetFileConfiguration;
import com.lambda.investing.market_data_connector.parquet_file_reader.ParquetMarketDataConnectorPublisher;
import com.lambda.investing.model.messaging.TypeMessage;
import com.lambda.investing.trading_engine_connector.TradingEngineConnector;
import com.lambda.investing.trading_engine_connector.ordinary.OrdinaryTradingEngine;
import com.lambda.investing.trading_engine_connector.paper.PaperExecutionReportConnectorPublisher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;


public class OrdinaryBacktest extends AbstractBacktest {

	public static int THREADS_PUBLISHING_MARKET_DATA_FILE = 1;//
	public static int THREADS_PUBLISHING_ORDER_REQUEST = 0;
	public static int THREADS_PUBLISHING_MARKETDATA = 1;
	public static int THREADS_PUBLISHING_EXECUTION_REPORTS = 1;
	public static int THREADS_LISTENING_EXECUTION_REPORTS = 1;
	public static int THREADS_LISTENING_ORDER_REQUEST = 1;
	private boolean isSingleThread = true;

	private OrdinaryConnectorConfiguration ordinaryConnectorConfiguration = new OrdinaryConnectorConfiguration();

	public OrdinaryBacktest(BacktestConfiguration backtestConfiguration) throws Exception {
		super(backtestConfiguration);

	}

	public void registerEndOfFile(MarketDataConnectorPublisherListener marketDataConnectorPublisherListener) {
		this.paperConnectorMarketDataAndExecutionReportPublisher.register(marketDataConnectorPublisherListener);
	}

	public void setSingleThread(boolean singleThread) {
		isSingleThread = singleThread;
		if (singleThread) {
			THREADS_PUBLISHING_MARKET_DATA_FILE = 0;
			THREADS_PUBLISHING_ORDER_REQUEST = 0;
			THREADS_PUBLISHING_MARKETDATA = 0;
			THREADS_PUBLISHING_EXECUTION_REPORTS = 0;
			THREADS_LISTENING_EXECUTION_REPORTS = 0;
			THREADS_LISTENING_ORDER_REQUEST = 0;
		}
	}

	@Override protected void constructPaperExecutionReportConnectorPublisher() {
		paperExecutionReportConnectorPublisher = new PaperExecutionReportConnectorPublisher(paperTradingEngine,
				ordinaryMarketDataConnectorProvider, backtestOrderRequestProvider, tradingEngineConnectorConfiguration);
		paperTradingEngine = getPaperTradingEngine();
		paperExecutionReportConnectorPublisher.setTradingEngineConnector(paperTradingEngine);
	}

	@Override protected void afterConstructor() {
		super.afterConstructor();

		//register rest of provides
		if (backtestMarketDataAndExecutionReportPublisher instanceof OrdinaryConnectorPublisherProvider) {
			OrdinaryConnectorPublisherProvider ordinaryConnectorPublisherProvider = (OrdinaryConnectorPublisherProvider) backtestMarketDataAndExecutionReportPublisher;
			if (paperTradingEngine instanceof OrdinaryTradingEngine) {
				OrdinaryTradingEngine ordinaryTradingEngine = (OrdinaryTradingEngine) paperTradingEngine;
				ordinaryConnectorPublisherProvider
						.register(new OrdinaryConnectorConfiguration(), ordinaryTradingEngine);
			}

			if (algorithmMarketDataProvider instanceof OrdinaryMarketDataProvider) {
				OrdinaryMarketDataProvider ordinaryMarketDataProvider = (OrdinaryMarketDataProvider) algorithmMarketDataProvider;
				ordinaryConnectorPublisherProvider
						.register(new OrdinaryConnectorConfiguration(), ordinaryMarketDataProvider);
			}
		}

	}

	@Override protected MarketDataProvider getAlgorithmMarketDataProvider() {
		OrdinaryConnectorPublisherProvider ordinaryConnectorPublisherProvider = new OrdinaryConnectorPublisherProvider(
				"backtest_md_publisher", THREADS_PUBLISHING_MARKET_DATA_FILE, Thread.MIN_PRIORITY);
		OrdinaryConnectorConfiguration ordinaryConnectorConfiguration = new OrdinaryConnectorConfiguration();
		OrdinaryMarketDataProvider ordinaryMD = new OrdinaryMarketDataProvider(ordinaryConnectorPublisherProvider,
				ordinaryConnectorConfiguration);

		return ordinaryMD;
	}

	@Override protected TradingEngineConnector getPaperTradingEngine() {
		return new OrdinaryTradingEngine((OrdinaryConnectorPublisherProvider) backtestOrderRequestProvider,
				paperExecutionReportConnectorPublisher, THREADS_PUBLISHING_ORDER_REQUEST,
				THREADS_LISTENING_EXECUTION_REPORTS);
	}

	@Override protected ConnectorProvider getBacktestOrderRequestProvider() {
		OrdinaryConnectorPublisherProvider ordinaryConnectorPublisherProvider = new OrdinaryConnectorPublisherProvider(
				"ordinaryOrderRequestProvider", THREADS_LISTENING_ORDER_REQUEST, Thread.NORM_PRIORITY);
		return ordinaryConnectorPublisherProvider;
	}

	protected void readFiles() {
		if (algorithmMarketDataProvider instanceof OrdinaryMarketDataProvider) {
			OrdinaryMarketDataProvider ordinaryMarketDataProvider = (OrdinaryMarketDataProvider) algorithmMarketDataProvider;
			ordinaryMarketDataProvider.init();
		}

		if (paperTradingEngine instanceof OrdinaryTradingEngine) {
			OrdinaryTradingEngine ordinaryTradingEngine = (OrdinaryTradingEngine) paperTradingEngine;
			//			ordinaryTradingEngine.();
		}

		if (ordinaryMarketDataConnectorPublisher instanceof CSVMarketDataConnectorPublisher) {
			CSVMarketDataConnectorPublisher csvMarketDataConnectorPublisher = (CSVMarketDataConnectorPublisher) this.ordinaryMarketDataConnectorPublisher;
			csvMarketDataConnectorPublisher.init();
		} else if (ordinaryMarketDataConnectorPublisher instanceof ParquetMarketDataConnectorPublisher) {
			ParquetMarketDataConnectorPublisher parquetMarketDataConnectorPublisher = (ParquetMarketDataConnectorPublisher) this.ordinaryMarketDataConnectorPublisher;
			parquetMarketDataConnectorPublisher.init();
		} else {
			logger.error(
					"cant read files : ordinaryMarketDataConnectorPublisher in CSVZeroMqBacktest is not CSVMarketDataConnectorPublisher");
		}
	}


	@Override protected ConnectorConfiguration getMarketDataConnectorConfiguration() {
		return ordinaryConnectorConfiguration;
	}

	@Override protected ConnectorConfiguration getTradingEngineConnectorConfiguration() {
		return ordinaryConnectorConfiguration;
	}

	@Override protected ConnectorPublisher getBacktestMarketDataAndExecutionReportConnectorPublisher() {
		OrdinaryConnectorPublisherProvider ordinaryConnectorPublisherProvider = new OrdinaryConnectorPublisherProvider(
				"OrdinaryBacktest", THREADS_PUBLISHING_MARKETDATA, Thread.MIN_PRIORITY);

		if (THREADS_PUBLISHING_EXECUTION_REPORTS != 0) {
			Map<TypeMessage, ThreadPoolExecutor> routingMap = new HashMap<>();

			//ER has max priority on threadpools
			ThreadPoolExecutor erThreadPoolExecutor = (ThreadPoolExecutor) Executors
					.newFixedThreadPool(THREADS_PUBLISHING_EXECUTION_REPORTS);
			ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
			threadFactoryBuilder.setNameFormat("ExecutionReportPublisher -%d").build();
			threadFactoryBuilder.setPriority(Thread.MAX_PRIORITY);

			erThreadPoolExecutor.setThreadFactory(threadFactoryBuilder.build());

			routingMap.put(TypeMessage.execution_report, erThreadPoolExecutor);
			ordinaryConnectorPublisherProvider.setRoutingPool(routingMap);
		}

		return ordinaryConnectorPublisherProvider;
	}
}
