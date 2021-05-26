package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.trading.ExecutionReport;
import com.lambda.investing.model.trading.ExecutionReportStatus;
import com.lambda.investing.model.trading.Verb;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.shaded.com.google.common.collect.EvictingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lambda.investing.algorithmic_trading.TimeseriesUtils.GetStd;
import static com.lambda.investing.algorithmic_trading.TimeseriesUtils.GetZscore;

@Getter public class PnlSnapshot {

	private static boolean CHECK_OPEN_PNL = true;

	Logger logger = LogManager.getLogger(PnlSnapshot.class);
	public double netPosition, avgOpenPrice, netInvestment, realizedPnl, unrealizedPnl, totalPnl, lastPriceForUnrealized;
	public Map<Double, Double> openPriceToVolume;
	public Map<Long, Double> historicalNetPosition, historicalAvgOpenPrice, historicalNetInvestment, historicalRealizedPnl, historicalUnrealizedPnl, historicalTotalPnl, historicalPrice, historicalQuantity;
	public Map<Long, List<CustomColumn>> historicalCustomColumns;
	public Map<Long, String> historicalVerb;
	public Map<Long, Integer> historicalNumberOfTrades;
	private Map<String, ExecutionReportStatus> processedClOrdId;
	private boolean nextCustomReject = false;
	public double lastPrice, lastQuantity;
	private long lastTimestampUpdate;
	private long lastTimestampExecutionReportUpdate = 0;
	public String lastVerb;
	public AtomicInteger numberOfTrades = new AtomicInteger(0);
	private boolean isBacktest = false;
	private boolean isPaper = false;
	private Queue<Double> midpricesQueue;
	private double maxExecutionPriceValid = Double.MAX_VALUE;
	private double minExecutionPriceValid = -Double.MAX_VALUE;
	private int windowTick = 10;
	private double stdMidPrice = 0.0;

	public PnlSnapshot() {
		openPriceToVolume = new ConcurrentHashMap<>();
		historicalCustomColumns = new ConcurrentHashMap<>();

		historicalNetPosition = new ConcurrentHashMap<>();
		historicalAvgOpenPrice = new ConcurrentHashMap<>();
		historicalNetInvestment = new ConcurrentHashMap<>();
		historicalRealizedPnl = new ConcurrentHashMap<>();
		historicalUnrealizedPnl = new ConcurrentHashMap<>();
		historicalTotalPnl = new ConcurrentHashMap<>();
		processedClOrdId = new ConcurrentHashMap<>();

		historicalPrice = new ConcurrentHashMap<>();
		historicalQuantity = new ConcurrentHashMap<>();
		historicalVerb = new ConcurrentHashMap<>();
		historicalNumberOfTrades = new ConcurrentHashMap<>();

		midpricesQueue = EvictingQueue.create(windowTick);
	}

	public void setNumberOfTrades(int numberOfTrades) {
		this.numberOfTrades = new AtomicInteger(numberOfTrades);
	}

	public void setNetPosition(double netPosition) {
		this.netPosition = netPosition;
	}

	public void setBacktest(boolean backtest) {
		isBacktest = backtest;
	}

	public void setPaper(boolean paper) {
		isPaper = paper;
	}

	private boolean checkUnrealizedHistorical(double unrealizedPnlProposal) {
		if (historicalUnrealizedPnl.size() < 25) {
			return true;
		}
		List<Double> unrealizedPnlList = new ArrayList<Double>(historicalUnrealizedPnl.values());
		//subsample it
		int startIndex = Math.max(0, unrealizedPnlList.size() - 100);//its inclusive
		int endIndex = unrealizedPnlList.size();//its exclusive
		unrealizedPnlList = unrealizedPnlList.subList(startIndex, endIndex);

		Double[] unrealizedPnlArr = new Double[unrealizedPnlList.size()];
		unrealizedPnlArr = unrealizedPnlList.toArray(unrealizedPnlArr);
		double zscore = GetZscore(unrealizedPnlArr, unrealizedPnlProposal);
		double maxZscoreWarning = 4;
		if (Math.abs(zscore) > maxZscoreWarning) {
			logger.warn("something is wrong on this unrealized Pnl {} zscore {}>{}", unrealizedPnlProposal, zscore,
					maxZscoreWarning);
			return false;
		}
		return true;
	}

	private void updateHistoricals(Long timestamp) {
		if (numberOfTrades.get() > 0 && timestamp > 0) {
			//			if (!checkUnrealizedHistorical(unrealizedPnl)) {
			//				//unacceptable mare than 15 zscores directly should be an error
			//				nextCustomReject=true;
			//				return;
			//			}

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
		//		if(nextCustomReject){
		//			nextCustomReject=false;
		//			return;
		//		}
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

		boolean validQuantity = !(executionReport.getLastQuantity() == 0 || Double
				.isNaN(executionReport.getLastQuantity()) || Double.isInfinite(executionReport.getLastQuantity()));

		if (!validQuantity) {
			logger.warn("cant update trade in portfolio manager with lastQuantity {}",
					executionReport.getLastQuantity());
			return;
		}
		boolean validPrice = !(Double.isNaN(executionReport.getPrice()) || Double
				.isInfinite(executionReport.getPrice()));

		if (!validPrice) {
			logger.warn("cant update trade in portfolio manager with not valid price {}", executionReport.getPrice());
			return;
		}
		if (lastTimestampExecutionReportUpdate != 0
				&& executionReport.getTimestampCreation() < lastTimestampExecutionReportUpdate) {
			logger.warn("execution report received of the past {}", executionReport);
		}
		if (processedClOrdId.containsKey(executionReport.getClientOrderId()) && processedClOrdId
				.get(executionReport.getClientOrderId()).equals(ExecutionReportStatus.CompletellyFilled)) {
			logger.warn("cant update trade in portfolio manager {} already processed with status {} received {} ",
					executionReport.getClientOrderId(), processedClOrdId.get(executionReport.getClientOrderId()),
					executionReport.getExecutionReportStatus());
			return;
		}

		//check only for live trading
		long currentTime = System.currentTimeMillis();
		if (!isBacktest && !isPaper && (currentTime - executionReport.getTimestampCreation()) > 60 * 60 * 1000) {
			logger.error(
					"something is wrong a lot of time since execution report was sent! , more than 1 hour?   {}>{}",
					new Date(currentTime), new Date(executionReport.getTimestampCreation()));
		}
		//

		//		if ((currentTime - executionReport.getTimestampCreation()<1000*60)) {
		//			logger.error("something is wrong  currentTime less ER time  {}<{}",new Date(currentTime),new Date( executionReport.getTimestampCreation()));
		//
		//		}

		lastPrice = executionReport.getPrice();
		lastQuantity = executionReport.getLastQuantity();
		lastVerb = executionReport.getVerb().name();

		lastPrice = Math.min(lastPrice, maxExecutionPriceValid);
		lastPrice = Math.max(lastPrice, minExecutionPriceValid);

		double quantityWithDirection = executionReport.getLastQuantity();
		if (executionReport.getVerb().equals(Verb.Sell)) {
			quantityWithDirection = -1 * quantityWithDirection;
		}

		boolean isStillOpen = (netPosition * quantityWithDirection) >= 0;

		//			net investment
		netInvestment = Math.max(netInvestment, Math.abs(netPosition * avgOpenPrice));
		//			realizedPnl
		if (!isStillOpen) {
			realizedPnl +=
					(lastPrice - avgOpenPrice) * Math.min(Math.abs(quantityWithDirection), Math.abs(netPosition)) * (
							Math.abs(netPosition) / netPosition);
		}

		//			totalPnl
		totalPnl = realizedPnl + unrealizedPnl;

		//			avg open price

		double prevAvgOpenPrice = avgOpenPrice;
		if (isStillOpen) {
			double newAvgOpenPrice = avgOpenPrice;
			if (!Double.isFinite(newAvgOpenPrice)) {
				newAvgOpenPrice = prevAvgOpenPrice;
			}
			double positionUpdatedPnl = newAvgOpenPrice * netPosition;
			double newOpenCapital = positionUpdatedPnl + (lastPrice * quantityWithDirection);
			double totalPosition = (netPosition + quantityWithDirection);
			avgOpenPrice = newOpenCapital / totalPosition;

		} else {
			if (executionReport.getLastQuantity() > Math.abs(netPosition)) {
				avgOpenPrice = lastPrice;
			}
		}

		//net position
		netPosition += quantityWithDirection;

		//number of trades
		numberOfTrades.incrementAndGet();
		lastTimestampExecutionReportUpdate = executionReport.getTimestampCreation();
		//historical
		updateHistoricals(executionReport.getTimestampCreation());
		processedClOrdId.put(executionReport.getClientOrderId(), executionReport.getExecutionReportStatus());

	}

	public void updateDepth(Depth depth) {
		if (!depth.isDepthFilled()) {
			return;
		}

		double lastPrice = depth.getMidPrice();
		if (lastPrice != 0 && avgOpenPrice != 0 && Double.isFinite(lastPrice) && Double.isFinite(avgOpenPrice)) {
			lastPriceForUnrealized = lastPrice;
			boolean lastPriceSideFound = false;

			if (netPosition > 0) {
				int levelFill = 0;
				double qtyLeft = Math.abs(netPosition);
				double priceTotal = 0.;
				while (levelFill < depth.getBidLevels()) {
					qtyLeft -= depth.getBidsQuantities()[levelFill];
					priceTotal += depth.getBids()[levelFill];
					levelFill++;
					if (qtyLeft <= 0) {
						break;
					}
				}
				if (levelFill > 0) {
					lastPriceForUnrealized = priceTotal / levelFill;
					lastPriceSideFound = true;
				}

			} else if (netPosition < 0) {
				int levelFill = 0;
				double qtyLeft = Math.abs(netPosition);
				double priceTotal = 0.;
				while (levelFill < depth.getAskLevels()) {
					qtyLeft -= depth.getAsksQuantities()[levelFill];
					priceTotal += depth.getAsks()[levelFill];
					levelFill++;
					if (qtyLeft <= 0) {
						break;
					}
				}
				if (levelFill > 0) {
					lastPriceForUnrealized = priceTotal / levelFill;
					lastPriceSideFound = true;
				}
			}

			if (!lastPriceSideFound) {
				lastPriceForUnrealized = depth.getMidPrice();//to have something negative at least
			}

			double unrealizedPnlProposal = (lastPriceForUnrealized - avgOpenPrice) * netPosition;
			boolean isOnLimitsOfPnl = true;
			if (CHECK_OPEN_PNL) {
				isOnLimitsOfPnl = checkUnrealizedHistorical(unrealizedPnlProposal);//remove here
			}
			if (!isOnLimitsOfPnl) {
				//dont update unrealized to not distort results open pnl plots
				logger.warn("unrealizedPnlProposal is out of bounds {} => using previous {}", unrealizedPnlProposal,
						unrealizedPnl);
			} else {
				unrealizedPnl = unrealizedPnlProposal;
				midpricesQueue.add(lastPrice);
				calculateBoundariesPrice(lastPriceForUnrealized);
			}

		}
		totalPnl = unrealizedPnl + realizedPnl;
		updateHistoricals(depth.getTimestamp());

	}

	private Double getStdMidPrice() {
		if (midpricesQueue == null) {
			return 0.0;
		}
		if (midpricesQueue.size() < windowTick) {
			return 0.0;
		}
		if (stdMidPrice != 0) {
			//calculate once only
			return stdMidPrice;
		}
		Double[] midPricesArr = new Double[windowTick];
		try {
			Double[] midPricesArrtemp = new Double[midpricesQueue.size()];
			midPricesArrtemp = midpricesQueue.toArray(midPricesArrtemp);
			if (midpricesQueue.size() == windowTick) {
				midPricesArr = midPricesArrtemp;
			} else {
				int indexArr = windowTick - 1;
				for (int lastElements = midPricesArrtemp.length - 1; lastElements >= 0; lastElements--) {
					midPricesArr[indexArr] = midPricesArrtemp[lastElements];
					indexArr--;
					if (indexArr < 0) {
						break;
					}
				}
			}

		} catch (IndexOutOfBoundsException e) {
			logger.error("error calculating std on {} windows tick with size {}-> return last stdMidPrice", windowTick,
					midpricesQueue.size());
			return this.stdMidPrice;
		}

		double std = GetStd(midPricesArr);
		//		double sum = 0.;
		//		for (int i = 0; i < windowTick; i++) {
		//			sum += midPricesArr[i];
		//		}
		//		double mean = sum / (double) windowTick;
		//		double sqDiff = 0;
		//		for (int i = 0; i < windowTick; i++) {
		//			sqDiff += (midPricesArr[i] - mean) * (midPricesArr[i] - mean);
		//		}
		//		double var = (double) sqDiff / windowTick;
		//		double std = Math.sqrt(var);
		return std;
	}

	private void calculateBoundariesPrice(double lastPrice) {
		this.stdMidPrice = getStdMidPrice();
		if (this.stdMidPrice == 0.0) {
			return;
		}
		minExecutionPriceValid = lastPrice - 10 * this.stdMidPrice;
		maxExecutionPriceValid = lastPrice + 10 * this.stdMidPrice;

	}

}