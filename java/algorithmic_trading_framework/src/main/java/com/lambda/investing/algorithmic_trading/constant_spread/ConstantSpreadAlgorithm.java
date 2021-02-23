package com.lambda.investing.algorithmic_trading.constant_spread;


import com.lambda.investing.algorithmic_trading.AlgorithmConnectorConfiguration;
import com.lambda.investing.algorithmic_trading.SingleInstrumentAlgorithm;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.exception.LambdaTradingException;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.trading.*;
import org.apache.commons.math3.util.Precision;

import java.util.Map;

public class ConstantSpreadAlgorithm extends SingleInstrumentAlgorithm {

	private static double MAX_TICKS_MIDPRICE_PRICE_DEV = 100;
	private double spreadMultiplier;
	private double quantity;

	private double lastValidSpread, lastValidMid = 0.01;

	public ConstantSpreadAlgorithm(AlgorithmConnectorConfiguration algorithmConnectorConfiguration,
			String algorithmInfo, Map<String, Object> parameters) {
		super(algorithmConnectorConfiguration, algorithmInfo, parameters);

		this.spreadMultiplier = Double.valueOf((String) parameters.get("spread_multiplier"));
		this.quantity = Double.valueOf((String) parameters.get("quantity"));
	}

	public ConstantSpreadAlgorithm(String algorithmInfo, Map<String, Object> parameters) {
		super(algorithmInfo, parameters);
		this.spreadMultiplier = Double.valueOf((String) parameters.get("spread_multiplier"));
		this.quantity = Double.valueOf((String) parameters.get("quantity"));

	}

	public void setInstrument(Instrument instrument) {
		this.instrument = instrument;
	}

	@Override public void setParameters(Map<String, Object> parameters) {
		super.setParameters(parameters);
		this.spreadMultiplier = Double.valueOf((String) parameters.get("spread_multiplier"));
		this.quantity = Double.valueOf((String) parameters.get("quantity"));
	}

	@Override public void setParameter(String name, Object value) {
		super.setParameter(name, value);

		this.spreadMultiplier = Double.valueOf((String) parameters.get("spread_multiplier"));
		this.quantity = Double.valueOf((String) parameters.get("quantity"));
	}

	@Override public String printAlgo() {
		return String
				.format("%s  spreadMultiplier=%.3f    quantity=%.5f   ", algorithmInfo, spreadMultiplier, quantity);
	}

	@Override public boolean onDepthUpdate(Depth depth) {
		//		if (!super.onDepthUpdate(depth) || !depth.isDepthFilled()) {
		//			stop();
		//			return false;
		//		} else {
		//			start();
		//		}

		try {
			double currentSpread = 0;
			double midPrice = 0;
			try {
				currentSpread = depth.getSpread();
				midPrice = depth.getMidPrice();
			} catch (Exception e) {
				return false;
			}

			if (currentSpread == 0) {
				currentSpread = lastValidSpread;
			} else {
				lastValidSpread = currentSpread;
			}

			if (midPrice == 0) {
				midPrice = lastValidMid;
			} else {
				lastValidMid = midPrice;
			}

			double askPrice = midPrice + (currentSpread / 2.) * spreadMultiplier;
			askPrice = Precision.round(askPrice, instrument.getNumberDecimalsPrice());
			double bidPrice = midPrice - (currentSpread / 2.) * spreadMultiplier;
			bidPrice = Precision.round(bidPrice, instrument.getNumberDecimalsPrice());

			//Check not crossing the mid price!
			askPrice = Math.max(askPrice, depth.getMidPrice() + instrument.getPriceTick());
			bidPrice = Math.min(bidPrice, depth.getMidPrice() - instrument.getPriceTick());

			//			Check worst price
			//			double maxAskPrice = depth.getMidPrice() + MAX_TICKS_MIDPRICE_PRICE_DEV * instrument.getPriceTick();
			//			askPrice = Math.min(askPrice, maxAskPrice);
			//			double minBidPrice = depth.getMidPrice() - MAX_TICKS_MIDPRICE_PRICE_DEV * instrument.getPriceTick();
			//			bidPrice = Math.max(bidPrice, minBidPrice);


			//create quote request
			QuoteRequest quoteRequest = createQuoteRequest(this.instrument);
			quoteRequest.setQuoteRequestAction(QuoteRequestAction.On);
			quoteRequest.setBidPrice(bidPrice);
			quoteRequest.setAskPrice(askPrice);
			quoteRequest.setBidQuantity(this.quantity);
			quoteRequest.setAskQuantity(this.quantity);

			try {
				sendQuoteRequest(quoteRequest);

				//				logger.info("quoting  {} bid {}@{}   ask {}@{}", instrument.getPrimaryKey(), quantity, bidPrice,
				//						quantity, askPrice);

			} catch (LambdaTradingException e) {
				logger.error("can't quote {} bid {}@{}   ask {}@{}", instrument.getPrimaryKey(), quantity, bidPrice,
						quantity, askPrice, e);
			}
		} catch (Exception e) {
			logger.error("error onDepth constant Spread : ", e);
		}

		return true;
	}

	@Override protected void sendOrderRequest(OrderRequest orderRequest) throws LambdaTradingException {
		//		logger.info("sendOrderRequest {} {}", orderRequest.getOrderRequestAction(), orderRequest.getClientOrderId());
		super.sendOrderRequest(orderRequest);

	}

	@Override public boolean onExecutionReportUpdate(ExecutionReport executionReport) {
		super.onExecutionReportUpdate(executionReport);

		//		logger.info("onExecutionReportUpdate  {}  {}:  {}", executionReport.getExecutionReportStatus(),
		//				executionReport.getClientOrderId(), executionReport.getRejectReason());

		//		boolean isTrade = executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled)
		//				|| executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.PartialFilled);

		//		if (isTrade) {
		//			try {
		//				//				logger.info("{} received {}  {}@{}",executionReport.getExecutionReportStatus(),executionReport.getVerb(),executionReport.getLastQuantity(),executionReport.getPrice());
		//				QuoteRequest quoteRequest = createQuoteRequest(executionReport.getInstrument());
		//				quoteRequest.setQuoteRequestAction(QuoteRequestAction.Off);
		//				sendQuoteRequest(quoteRequest);
		//				//				logger.info("unquoting because of trade in {} {}", executionReport.getVerb(),
		//				//						executionReport.getClientOrderId());
		//			} catch (LambdaTradingException e) {
		//				logger.error("cant unquote {}", instrument.getPrimaryKey(), e);
		//			}
		//		}
		return true;
	}
}
