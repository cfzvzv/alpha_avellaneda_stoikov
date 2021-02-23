package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.exception.LambdaTradingException;
import com.lambda.investing.model.trading.*;
import com.lambda.investing.trading_engine_connector.ExecutionReportListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.lambda.investing.algorithmic_trading.Algorithm.EXECUTION_REPORT_LOCK;

public class QuoteManager implements ExecutionReportListener, Runnable {

	private static boolean UPDATE_QUOTE_BUFFER_THREAD = false;//if true will be update timely on a different thread

	Logger logger = LogManager.getLogger(QuoteManager.class);
	private Algorithm algorithm;
	private Instrument instrument;

	private QuoteRequest lastQuoteRequest;
	private QuoteSideManager bidQuoteSideManager;
	private QuoteSideManager askQuoteSideManager;

	private int limitOrders = 2;

	public void setLimitOrders(int limitOrders) {
		this.limitOrders = limitOrders;
	}

	Thread quoteThread;

	public QuoteManager(Algorithm algorithm, Instrument instrument) {
		this.algorithm = algorithm;
		this.instrument = instrument;
		this.bidQuoteSideManager = new QuoteSideManager(this.algorithm, this.instrument, Verb.Buy);
		this.askQuoteSideManager = new QuoteSideManager(this.algorithm, this.instrument, Verb.Sell);

		if (UPDATE_QUOTE_BUFFER_THREAD) {
			quoteThread = new Thread(this, "quoteManager");
			quoteThread.setPriority(Thread.MIN_PRIORITY);
			quoteThread.start();
		}

	}

	public void reset() {
		this.bidQuoteSideManager.reset();
		this.askQuoteSideManager.reset();
	}

	public void quoteRequest(QuoteRequest quoteRequest) throws LambdaTradingException {
		this.lastQuoteRequest = quoteRequest;
		if (!UPDATE_QUOTE_BUFFER_THREAD) {
			try {
				updateQuote();
			} catch (Exception e) {
				//				logger.error(e);
			}
		}
	}

	private void updateQuote() throws LambdaTradingException {
		checkQuoteRequest(this.lastQuoteRequest);
		LambdaTradingException ex = null;
		try {
			bidQuoteSideManager.quoteRequest(this.lastQuoteRequest);
		} catch (Exception e) {
			ex = new LambdaTradingException(e);
		}

		askQuoteSideManager.quoteRequest(this.lastQuoteRequest);
		if (ex != null) {
			throw ex;
		}
	}

	private void checkQuoteRequest(QuoteRequest quoteRequest) throws LambdaTradingException {
		if (Double.isNaN(quoteRequest.getAskPrice()) || Double.isInfinite(quoteRequest.getAskPrice())) {
			throw new LambdaTradingException(
					"Wrong ask price " + quoteRequest.getAskPrice() + " in " + quoteRequest.getInstrument());
		}

		if (Double.isNaN(quoteRequest.getBidPrice()) || Double.isInfinite(quoteRequest.getBidPrice())) {
			throw new LambdaTradingException(
					"Wrong bid price " + quoteRequest.getBidPrice() + " in " + quoteRequest.getInstrument());
		}

		Map<String, ExecutionReport> instrumentActiveOrders = algorithm.getActiveOrders(this.instrument);
		Map<String, OrderRequest> instrumentRequestOrders = algorithm.getRequestOrders(this.instrument);

		if (instrumentRequestOrders.size() > limitOrders) {
			String requestOrders = "";
			for (OrderRequest orderRequest : instrumentRequestOrders.values()) {
				requestOrders += String.format("\n%s [%s]  %s %.5f@%.5f", orderRequest.getOrderRequestAction(),
						orderRequest.getClientOrderId(), orderRequest.getVerb(), orderRequest.getQuantity(),
						orderRequest.getPrice());
			}
			logger.error("more than {} request pending! {} {}", limitOrders, instrumentRequestOrders.size(),
					requestOrders);
			logger.error(algorithm.getLastDepth(instrument).prettyPrint());
			throw new LambdaTradingException("cant quote with more than limitOrders request orders pending ER");
		}
		if (instrumentActiveOrders.size() > limitOrders) {
			String activeOrders = "";
			for (ExecutionReport executionReport : instrumentActiveOrders.values()) {
				activeOrders += String.format("\n%s [%s]  %s %.5f@%.5f", executionReport.getExecutionReportStatus(),
						executionReport.getClientOrderId(), executionReport.getVerb(), executionReport.getQuantity(),
						executionReport.getPrice());
			}
			logger.error(algorithm.getLastDepth(instrument).prettyPrint());
			logger.error("more than {} request active! {} {}", limitOrders, instrumentActiveOrders.size(),
					activeOrders);
			throw new LambdaTradingException("cant quote with more than limitOrders orders active");
		}

	}

	@Override public boolean onExecutionReportUpdate(ExecutionReport executionReport) {
		bidQuoteSideManager.onExecutionReportUpdate(executionReport);
		askQuoteSideManager.onExecutionReportUpdate(executionReport);
		return true;

	}

	public void unquote() throws LambdaTradingException {
		bidQuoteSideManager.unquoteSide();
		askQuoteSideManager.unquoteSide();
	}

	public void unquoteSide(Verb verb) throws LambdaTradingException {
		if (verb.equals(Verb.Buy)) {
			bidQuoteSideManager.unquoteSide();
		} else {
			askQuoteSideManager.unquoteSide();
		}

	}

	@Override public void run() {
		while (true) {

			try {
				updateQuote();
			} catch (Exception e) {

			}

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
