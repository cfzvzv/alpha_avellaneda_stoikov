package com.lambda.investing.trading_engine_connector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorListener;
import com.lambda.investing.connector.zero_mq.ZeroMqConfiguration;
import com.lambda.investing.connector.zero_mq.ZeroMqProvider;
import com.lambda.investing.connector.zero_mq.ZeroMqPublisher;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.messaging.TopicUtils;
import com.lambda.investing.model.messaging.TypeMessage;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.OrderRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.PostConstruct;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ZeroMqTradingEngineConnector implements TradingEngineConnector, ConnectorListener {

	public static String ALL_ALGORITHMS_SUBSCRIPTION = "*";

	protected Logger logger = LogManager.getLogger(ZeroMqTradingEngineConnector.class);
	private ZeroMqConfiguration zeroMqConfigurationExecutionReportListening, zeroMqConfigurationOrderRequest;
	private ZeroMqProvider zeroMqExecutionReportProvider;
	private ZeroMqPublisher zeroMqPublisher;
	private ExecutionReportListener allAlgorithmsExecutionReportListener;

	public static Gson GSON = new GsonBuilder()
			.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT, Modifier.VOLATILE, Modifier.FINAL)
			.serializeSpecialFloatingPointValues().disableHtmlEscaping().create();

	protected Map<String, Map<ExecutionReportListener, String>> listenersManager;

	/***
	 * Trader engine for generic brokers
	 * @param name
	 * @param threadsPublish
	 * @param threadsListen
	 * @param zeroMqConfigurationExecutionReportListening
	 * @param zeroMqConfigurationOrderRequest
	 */
	public ZeroMqTradingEngineConnector(String name, int threadsPublish, int threadsListen, ZeroMqConfiguration zeroMqConfigurationExecutionReportListening,
			ZeroMqConfiguration zeroMqConfigurationOrderRequest) {
		this.zeroMqConfigurationExecutionReportListening = zeroMqConfigurationExecutionReportListening;
		//listen the answers here
		zeroMqExecutionReportProvider = ZeroMqProvider
				.getInstance(this.zeroMqConfigurationExecutionReportListening, threadsListen);
		zeroMqExecutionReportProvider.register(this.zeroMqConfigurationExecutionReportListening, this);
		logger.info("Listening ExecutionReports on {}   in tcp://{}:{}", zeroMqConfigurationExecutionReportListening.getTopic(),
				zeroMqConfigurationExecutionReportListening.getHost(), zeroMqConfigurationExecutionReportListening.getPort());

		//publish the request here
		this.zeroMqConfigurationOrderRequest = zeroMqConfigurationOrderRequest;
		this.zeroMqPublisher = new ZeroMqPublisher(name, threadsPublish);

		logger.info("Publishing OrderRequests on {}   in tcp://{}:{}", this.zeroMqConfigurationOrderRequest.getTopic(),
				this.zeroMqConfigurationOrderRequest.getHost(), this.zeroMqConfigurationOrderRequest.getPort());

		this.zeroMqPublisher
				.publish(this.zeroMqConfigurationOrderRequest, TypeMessage.command, "*", "starting publishing");

		listenersManager = new ConcurrentHashMap<>();
	}

	@PostConstruct public void start() {
		zeroMqExecutionReportProvider.start(true);
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

	@Override public void deregister(String algorithmInfo, ExecutionReportListener executionReportListener) {
		Map<ExecutionReportListener, String> insideMap = listenersManager
				.getOrDefault(algorithmInfo, new ConcurrentHashMap<>());
		insideMap.remove(executionReportListener);
		listenersManager.put(algorithmInfo, insideMap);
	}

	@Override public boolean orderRequest(OrderRequest orderRequest) {
		String topic = TopicUtils.getTopic(orderRequest.getInstrument(), TypeMessage.order_request);
		String message = GSON.toJson(orderRequest);
		this.zeroMqPublisher.publish(this.zeroMqConfigurationOrderRequest, TypeMessage.order_request, topic, message);
		return true;
	}

	@Override public void onUpdate(ConnectorConfiguration configuration, long timestampReceived,
			TypeMessage typeMessage, String content) {
		//ER read

		if (typeMessage.equals(TypeMessage.execution_report)) {
			ExecutionReport executionReport = GSON.fromJson(content, ExecutionReport.class);
			String algorithmInfo = executionReport.getAlgorithmInfo();
			Map<ExecutionReportListener, String> insideMap = listenersManager
					.getOrDefault(algorithmInfo, new ConcurrentHashMap<>());
			if (insideMap.size() > 0) {
				for (ExecutionReportListener executionReportListener : insideMap.keySet()) {
					executionReportListener.onExecutionReportUpdate(executionReport);
				}
			}
			if (allAlgorithmsExecutionReportListener != null) {
				allAlgorithmsExecutionReportListener.onExecutionReportUpdate(executionReport);
			}
		}

	}
}
