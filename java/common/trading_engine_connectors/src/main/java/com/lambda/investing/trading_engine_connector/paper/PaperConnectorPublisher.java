package com.lambda.investing.trading_engine_connector.paper;

import com.lambda.investing.connector.ConnectorConfiguration;
import com.lambda.investing.connector.ConnectorPublisher;
import com.lambda.investing.market_data_connector.AbstractMarketDataConnectorPublisher;

import com.lambda.investing.model.messaging.TopicUtils;
import com.lambda.investing.model.messaging.TypeMessage;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.trading_engine_connector.ExecutionReportPublisher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.lambda.investing.market_data_connector.AbstractMarketDataProvider.GSON;

/***
 * Will publish the market data after filling own orderbook and Execution reports
 */
public class PaperConnectorPublisher extends AbstractMarketDataConnectorPublisher implements ExecutionReportPublisher {

	protected Logger logger = LogManager.getLogger(PaperConnectorPublisher.class);

	public PaperConnectorPublisher(ConnectorConfiguration connectorConfiguration,
			ConnectorPublisher connectorPublisher) {
		super("PaperConnectorPublisher", connectorConfiguration, connectorPublisher);
		setStatistics(null);
	}

	@Override public void init() {

	}

	@Override public void notifyExecutionReport(ExecutionReport executionReport) {

		String topic = TopicUtils.getTopic(executionReport.getInstrument(), TypeMessage.execution_report);
		String executionReportJson = GSON.toJson(executionReport);
		topic = topic + "." + TypeMessage.execution_report.name();
		//		logger.debug("notify ER {}",executionReport);
		connectorPublisher.publish(connectorConfiguration, TypeMessage.execution_report, topic, executionReportJson);
		if (statistics != null) {
			statistics.addStatistics(topic);
		}
	}

}
