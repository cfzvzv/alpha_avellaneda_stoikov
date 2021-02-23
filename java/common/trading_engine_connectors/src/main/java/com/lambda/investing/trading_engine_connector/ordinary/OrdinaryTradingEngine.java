package com.lambda.investing.trading_engine_connector.ordinary;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorListener;
import com.lambda.investing.connector.ConnectorProvider;
import com.lambda.investing.connector.ThreadUtils;
import com.lambda.investing.connector.ordinary.OrdinaryConnectorConfiguration;
import com.lambda.investing.model.messaging.TypeMessage;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.OrderRequest;
import com.lambda.investing.trading_engine_connector.ExecutionReportListener;
import com.lambda.investing.trading_engine_connector.TradingEngineConnector;
import com.lambda.investing.trading_engine_connector.paper.PaperExecutionReportConnectorPublisher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import static com.lambda.investing.trading_engine_connector.ZeroMqTradingEngineConnector.ALL_ALGORITHMS_SUBSCRIPTION;
import static com.lambda.investing.trading_engine_connector.ZeroMqTradingEngineConnector.GSON;

public class OrdinaryTradingEngine implements TradingEngineConnector, ConnectorListener {

	private ConnectorProvider executionReportOrderRequestConnectorProvider;
	private PaperExecutionReportConnectorPublisher paperTradingEngineConnector;

	private OrdinaryConnectorConfiguration ordinaryConnectorConfiguration = new OrdinaryConnectorConfiguration();
	protected Map<String, Map<ExecutionReportListener, String>> listenersManager;
	private ExecutionReportListener allAlgorithmsExecutionReportListener;

	private int threadsSendOrderRequest, threadsListeningExecutionReports;

	ThreadFactory namedThreadFactoryOrderRequest = new ThreadFactoryBuilder()
			.setNameFormat("OrdinaryTradingEngine-OrderRequest-%d").build();

	ThreadFactory namedThreadFactoryExecutionReport = new ThreadFactoryBuilder()
			.setNameFormat("OrdinaryTradingEngine-ExecutionReport-%d").build();

	ThreadPoolExecutor senderPool, receiverPool;

	public OrdinaryTradingEngine(ConnectorProvider executionReportOrderRequestConnectorProvider,
			PaperExecutionReportConnectorPublisher paperTradingEngineConnector, int threadsSendOrderRequest,
			int threadsListeningExecutionReports) {
		this.executionReportOrderRequestConnectorProvider = executionReportOrderRequestConnectorProvider;
		this.paperTradingEngineConnector = paperTradingEngineConnector;
		listenersManager = new ConcurrentHashMap<>();

		ThreadFactoryBuilder threadFactoryBuilder1 = new ThreadFactoryBuilder();
		threadFactoryBuilder1.setNameFormat("OrdinaryTradingEngine-OrderRequest-%d");
		threadFactoryBuilder1.setPriority(Thread.NORM_PRIORITY);
		ThreadFactory namedThreadFactoryOrderRequest = threadFactoryBuilder1.build();

		ThreadFactoryBuilder threadFactoryBuilder2 = new ThreadFactoryBuilder();
		threadFactoryBuilder2.setNameFormat("OrdinaryTradingEngine-ExecutionReport-%d");
		threadFactoryBuilder2.setPriority(Thread.NORM_PRIORITY);
		ThreadFactory namedThreadFactoryExecutionReport = threadFactoryBuilder1.build();


		this.threadsSendOrderRequest = threadsSendOrderRequest;
		if (this.threadsSendOrderRequest > 0) {
			senderPool = (ThreadPoolExecutor) Executors
					.newFixedThreadPool(this.threadsSendOrderRequest, namedThreadFactoryOrderRequest);
		}
		if (this.threadsSendOrderRequest < 0) {
			senderPool = (ThreadPoolExecutor) Executors.newCachedThreadPool(namedThreadFactoryOrderRequest);
		}

		this.threadsListeningExecutionReports = threadsListeningExecutionReports;
		if (this.threadsListeningExecutionReports > 0) {
			receiverPool = (ThreadPoolExecutor) Executors
					.newFixedThreadPool(this.threadsListeningExecutionReports, namedThreadFactoryExecutionReport);
		}
		if (this.threadsListeningExecutionReports < 0) {
			receiverPool = (ThreadPoolExecutor) Executors.newCachedThreadPool(namedThreadFactoryExecutionReport);
		}

	}

	@Override public void register(String algorithmInfo, ExecutionReportListener executionReportListener) {
		Map<ExecutionReportListener, String> insideMap = listenersManager
				.getOrDefault(algorithmInfo, new ConcurrentHashMap<>());
		insideMap.put(executionReportListener, "");
		if (algorithmInfo.equalsIgnoreCase(ALL_ALGORITHMS_SUBSCRIPTION)) {
			allAlgorithmsExecutionReportListener = executionReportListener;
		}
		listenersManager.put(algorithmInfo, insideMap);
	}

	@Override public void deregister(String id, ExecutionReportListener executionReportListener) {
		this.executionReportOrderRequestConnectorProvider.deregister(ordinaryConnectorConfiguration, this);
	}

	@Override public boolean orderRequest(OrderRequest orderRequest) {
		if (this.threadsSendOrderRequest == 0) {
			return paperTradingEngineConnector.orderRequest(orderRequest);
		} else {
			senderPool.submit(() -> {
				paperTradingEngineConnector.orderRequest(orderRequest);
			});
			return true;
		}
	}

	@Override public void onUpdate(ConnectorConfiguration configuration, long timestampReceived,
			TypeMessage typeMessage, String content) {
		if (this.threadsListeningExecutionReports == 0) {
			_onUpdate(configuration, timestampReceived, typeMessage, content);
		} else {
			receiverPool.submit(() -> {
				_onUpdate(configuration, timestampReceived, typeMessage, content);
			});
		}

	}

	private void _onUpdate(ConnectorConfiguration configuration, long timestampReceived, TypeMessage typeMessage, String content) {

		if (typeMessage.equals(TypeMessage.execution_report)) {
			ExecutionReport executionReport = GSON.fromJson(content, ExecutionReport.class);
			String algorithmInfo = executionReport.getAlgorithmInfo();
			Map<ExecutionReportListener, String> insideMap = listenersManager.getOrDefault(algorithmInfo, new ConcurrentHashMap<>());
			if (insideMap.size() > 0) {
				for (ExecutionReportListener executionReportListener : insideMap.keySet()) {
					executionReportListener.onExecutionReportUpdate(executionReport);
				}
			}
			//			if (allAlgorithmsExecutionReportListener != null) {
			//				allAlgorithmsExecutionReportListener.onExecutionReportUpdate(executionReport);
			//			}
		}
	}
}
