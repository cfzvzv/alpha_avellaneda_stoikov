package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.Verb;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class PnlSnapshot {

	Logger logger = LogManager.getLogger(PnlSnapshot.class);
	public double netPosition, avgOpenPrice, netInvestment, realizedPnl, unrealizedPnl, totalPnl;
	public Map<Double, Double> openPriceToVolume;
	public Map<Long, Double> historicalNetPosition, historicalAvgOpenPrice, historicalNetInvestment, historicalRealizedPnl, historicalUnrealizedPnl, historicalTotalPnl, historicalPrice, historicalQuantity;
	public Map<Long, List<CustomColumn>> historicalCustomColumns;
	public Map<Long, String> historicalVerb;
	public Map<Long, Integer> historicalNumberOfTrades;

	public double lastPrice, lastQuantity;
	private long lastTimestampUpdate;
	public String lastVerb;
	public AtomicInteger numberOfTrades = new AtomicInteger(0);

	public PnlSnapshot() {
		openPriceToVolume = new ConcurrentHashMap<>();
		historicalCustomColumns = new ConcurrentHashMap<>();

		historicalNetPosition = new ConcurrentHashMap<>();
		historicalAvgOpenPrice = new ConcurrentHashMap<>();
		historicalNetInvestment = new ConcurrentHashMap<>();
		historicalRealizedPnl = new ConcurrentHashMap<>();
		historicalUnrealizedPnl = new ConcurrentHashMap<>();
		historicalTotalPnl = new ConcurrentHashMap<>();

		historicalPrice = new ConcurrentHashMap<>();
		historicalQuantity = new ConcurrentHashMap<>();
		historicalVerb = new ConcurrentHashMap<>();
		historicalNumberOfTrades = new ConcurrentHashMap<>();
	}

	private void updateHistoricals(Long timestamp) {
		if (numberOfTrades.get() > 0 && timestamp > 0) {
			lastTimestampUpdate = timestamp;
			historicalNetPosition.put(timestamp, netPosition);
			historicalAvgOpenPrice.put(timestamp, avgOpenPrice);
			historicalNetInvestment.put(timestamp, netInvestment);
			historicalRealizedPnl.put(timestamp, realizedPnl);
			historicalUnrealizedPnl.put(timestamp, unrealizedPnl);
			historicalTotalPnl.put(timestamp, totalPnl);

			historicalPrice.put(timestamp, lastPrice);
			historicalQuantity.put(timestamp, lastQuantity);
			historicalVerb.put(timestamp, lastVerb);
			historicalNumberOfTrades.put(timestamp, numberOfTrades.get());
		}

	}

	public void updateHistoricalsCustom(Long timestamp, String key, double value) {
		if (numberOfTrades.get() > 0 && timestamp > 0) {
			CustomColumn customColumn = new CustomColumn(key, value);
			List<CustomColumn> columnsList = historicalCustomColumns.getOrDefault(timestamp, new ArrayList<>());
			if (!columnsList.contains(customColumn)) {
				columnsList.add(customColumn);
			}
			historicalCustomColumns.put(timestamp, columnsList);
		}
	}


	public void setLastTimestampUpdate(long lastTimestampUpdate) {
		this.lastTimestampUpdate = lastTimestampUpdate;
	}

	public void updateExecutionReport(ExecutionReport executionReport) {

		boolean validQuantity = !(executionReport.getLastQuantity() == 0 || Double.isNaN(executionReport.getLastQuantity()) || Double.isInfinite(executionReport.getLastQuantity()));

		if (!validQuantity) {
			logger.warn("cant update trade in portfolio manager with lastQuantity {}", executionReport.getLastQuantity());
			return;
		}
		boolean validPrice = !(Double.isNaN(executionReport.getPrice()) || Double.isInfinite(executionReport.getPrice()));

		if (!validPrice) {
			logger.warn("cant update trade in portfolio manager with not valid price {}", executionReport.getPrice());
			return;
		}
		if ((System.currentTimeMillis() - executionReport.getTimestampCreation()) < 60 * 60 * 1000) {
			logger.error("something is wrong");
		}

		lastPrice = executionReport.getPrice();
		lastQuantity = executionReport.getLastQuantity();
		lastVerb = executionReport.getVerb().name();

		double quantityWithDirection = executionReport.getLastQuantity();
		if (executionReport.getVerb().equals(Verb.Sell)) {
			quantityWithDirection = -1 * quantityWithDirection;
		}

		boolean isStillOpen = (netPosition * quantityWithDirection) >= 0;

		//			net investment
		netInvestment = Math.max(netInvestment, Math.abs(netPosition * avgOpenPrice));
		//			realizedPnl
		if (!isStillOpen) {
			realizedPnl += (executionReport.getPrice() - avgOpenPrice) * Math.min(Math.abs(quantityWithDirection), Math.abs(netPosition)) * (Math.abs(netPosition)
					/ netPosition);
		}

		//			totalPnl
		totalPnl = realizedPnl + unrealizedPnl;

		//			avg open price

		double prevAvgOpenPrice = avgOpenPrice;
		if (isStillOpen) {
			if (!Double.isFinite(avgOpenPrice)) {
				avgOpenPrice = prevAvgOpenPrice;
			}
			double positionUpdatedPnl = avgOpenPrice * netPosition;
			double newOpenCapital = positionUpdatedPnl + (executionReport.getPrice() * quantityWithDirection);
			double totalPosition = (netPosition + quantityWithDirection);
			avgOpenPrice = newOpenCapital / totalPosition;

		} else {
			if (executionReport.getLastQuantity() > Math.abs(netPosition)) {
				avgOpenPrice = executionReport.getPrice();
			}
		}

		//net position
		netPosition += quantityWithDirection;

		//number of trades
		numberOfTrades.incrementAndGet();

		//historical
		updateHistoricals(executionReport.getTimestampCreation());

	}


	public void updateDepth(Depth depth) {
		if (!depth.isDepthFilled()) {
			return;
		}
		double lastPrice = depth.getMidPrice();
		if (lastPrice != 0 && Double.isFinite(lastPrice)) {
			unrealizedPnl = (lastPrice - avgOpenPrice) * netPosition;
		}
		totalPnl = unrealizedPnl + realizedPnl;
		updateHistoricals(depth.getTimestamp());

	}

}