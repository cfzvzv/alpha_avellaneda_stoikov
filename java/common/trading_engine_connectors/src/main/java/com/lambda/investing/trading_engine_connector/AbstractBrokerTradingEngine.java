package com.lambda.investing.trading_engine_connector;

import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorListener;
import com.lambda.investing.connector.ConnectorProvider;
import com.lambda.investing.connector.ConnectorPublisher;
import com.lambda.investing.model.messaging.TypeMessage;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.ExecutionReportStatus;
import com.lambda.investing.model.trading.OrderRequest;
import com.lambda.investing.model.trading.OrderRequestAction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.lambda.investing.trading_engine_connector.ZeroMqTradingEngineConnector.ALL_ALGORITHMS_SUBSCRIPTION;
import static com.lambda.investing.trading_engine_connector.ZeroMqTradingEngineConnector.GSON;

public abstract class AbstractBrokerTradingEngine implements TradingEngineConnector, ConnectorListener {

	protected static String REJECT_ORIG_NOT_FOUND_FORMAT = "origClientOrderId %s not found for %s in %s";//origClientOrderId , action,instrument



	protected ConnectorProvider orderRequestConnectorProvider;
	protected ConnectorConfiguration orderRequestConnectorConfiguration;

	protected ConnectorPublisher executionReportConnectorPublisher;
	protected ConnectorConfiguration executionReportConnectorConfiguration;

	protected Map<String, Map<ExecutionReportListener, String>> listenersManager;

	public AbstractBrokerTradingEngine(ConnectorConfiguration orderRequestConnectorConfiguration,
			ConnectorProvider orderRequestConnectorProvider,
			ConnectorConfiguration executionReportConnectorConfiguration,
			ConnectorPublisher executionReportConnectorPublisher) {
		this.orderRequestConnectorConfiguration = orderRequestConnectorConfiguration;
		this.orderRequestConnectorProvider = orderRequestConnectorProvider;
		this.executionReportConnectorConfiguration = executionReportConnectorConfiguration;
		this.executionReportConnectorPublisher = executionReportConnectorPublisher;

		listenersManager = new ConcurrentHashMap<>();
	}

	public void start() {
		//listening orderRequest
		this.orderRequestConnectorProvider.register(this.orderRequestConnectorConfiguration, this);

	}

	@Override public void register(String algorithmInfo, ExecutionReportListener executionReportListener) {
		//no sense on broker that are going to send the ER to connector publisher
	}

	@Override public void deregister(String id, ExecutionReportListener executionReportListener) {
		//no sense on broker that are going to send the ER to connector publisher
	}

	protected ExecutionReport createRejectionExecutionReport(OrderRequest orderRequest, String reason) {
		ExecutionReport executionReport = new ExecutionReport(orderRequest);
		if (orderRequest.getOrderRequestAction().equals(OrderRequestAction.Cancel)) {
			executionReport.setExecutionReportStatus(ExecutionReportStatus.CancelRejected);
		} else {

			executionReport.setExecutionReportStatus(ExecutionReportStatus.Rejected);
		}
		executionReport.setRejectReason(reason);
		return executionReport;
	}

	//called by extension when filled /partial filled
	protected void notifyExecutionReportById(ExecutionReport executionReport) {
		String id = executionReport.getAlgorithmInfo();
		this.executionReportConnectorPublisher
				.publish(executionReportConnectorConfiguration, TypeMessage.execution_report, id,
						GSON.toJson(executionReport));

	}

	//receiving OrderRequest
	@Override public void onUpdate(ConnectorConfiguration configuration, long timestampReceived,
			TypeMessage typeMessage, String content) {
		if (typeMessage.equals(TypeMessage.order_request)) {
			OrderRequest orderRequest = GSON.fromJson(content, OrderRequest.class);
			orderRequest(orderRequest);
		}

	}
}
