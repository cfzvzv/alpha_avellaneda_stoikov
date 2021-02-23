package com.lambda.investing.algorithmic_trading.avellaneda_stoikov;

import com.lambda.investing.algorithmic_trading.*;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.exception.LambdaTradingException;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;
import com.lambda.investing.model.trading.*;
import org.apache.commons.math3.util.Precision;
import org.apache.curator.shaded.com.google.common.collect.EvictingQueue;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * https://www.math.nyu.edu/faculty/avellane/HighFrequencyTrading.pdf
 * https://github.com/mdibo/Avellaneda-Stoikov/blob/master/AvellanedaStoikov.py
 * T = inf
 * δa , δb  are the bid and ask spreads
 * γ is a risk aversion parameter
 * XT is the cash at time T
 * qT is the inventory at time T
 * ST is the stock price at time T
 * k intensity of to be "hit" by a market orders
 * Assuming the risk-free rate is zero and the mid-price of a stock follows a
 * standard brownian motion dSt = σdWt with initial value S0 = s and standard deviation σ,
 * <p>
 * Avellaneda Stoikov also provides a structure to the number of bid and ask executions by modeling them as a Poisson process. According to their framework, this Poisson
 * process should also depend on the market depth of our quote
 * <p>
 * λ(δ) = Ae(−κδ)
 * <p>
 * reservation price -> r(s, t) = s − qγσ2*(T − t)
 * spread around reservation -> δa + δb = γσ2*(T − t) + ln(1 + γ/κ)
 * <p>
 * k can be estimated
 * https://quant.stackexchange.com/questions/36073/how-does-one-calibrate-lambda-in-a-avellaneda-stoikov-market-making-problem
 * <p>
 * from High Freq trading by Irene Alridge , page 139
 * k_bid = λb/delta(λb)  / delta(λb)=(λb-λb-1)/λb-1
 * :return: k_bid and k_ask tuple
 */
public class AvellanedaStoikov extends SingleInstrumentAlgorithm {

	private static boolean DISABLE_ON_HIT = false;
	private static double MAX_TICKS_MIDPRICE_PRICE_DEV = 100;
	protected double riskAversion;
	protected double quantity;
	protected int windowTick;
	protected double skewPricePct;
	protected boolean autoEnableSideTime = true;

	protected int minutesChangeK = 1;
	private double targetPosition = 0.;
	private double positionMultiplier = 1.;

	private Queue<Double> midpricesQueue;
	private Queue<Long> counterTradesPerMinute;
	private Queue<Long> counterBuyTradesPerMinute;
	private Queue<Long> counterSellTradesPerMinute;
	private long counterStartingMinuteMs = 0;
	private long counterTrades = 0;
	private long counterBuyTrades = 0;
	private long counterSellTrades = 0;
	private double spreadMultiplier = 1.;
	private Double kDefault, varianceMidPrice;

	private long stopTradeSideMs = 60 * 1000 * 5;//5 mins *60 seconds/min * 1000 ms /seconds
	protected Map<Verb, Boolean> sideActive;

	public void setKdefault(Double kDefault) {
		this.kDefault = kDefault;
	}

	public AvellanedaStoikov(AlgorithmConnectorConfiguration algorithmConnectorConfiguration, String algorithmInfo,
			Map<String, Object> parameters) {
		super(algorithmConnectorConfiguration, algorithmInfo, parameters);
		setParameters(parameters);

	}

	public AvellanedaStoikov(String algorithmInfo, Map<String, Object> parameters) {
		super(algorithmInfo, parameters);
		setParameters(parameters);
	}

	@Override public void reset() {
		super.reset();
		//		this.midpricesQueue = null;
	}

	public void setInstrument(Instrument instrument) {
		super.setInstrument(instrument);
		//		this.algorithmInfo += "_" + instrument.getPrimaryKey();
		//		this.algorithmNotifier.setAlgorithmInfo(this.algorithmInfo);
	}

	@Override public void setParameters(Map<String, Object> parameters) {
		super.setParameters(parameters);
		this.riskAversion = getParameterDouble(parameters, "risk_aversion");
		this.quantity = getParameterDouble(parameters, "quantity");
		this.windowTick = getParameterInt(parameters, "window_tick");
		this.targetPosition = getParameterDoubleOrDefault(parameters, "target_position", 0.);
		this.positionMultiplier = getParameterDoubleOrDefault(parameters, "position_multiplier", 1.);
		this.spreadMultiplier = getParameterDoubleOrDefault(parameters, "spread_multiplier",
				1.);//Double.valueOf((String) parameters.getOrDefault("spread_multiplier", "1."));
		this.kDefault = getParameterDoubleOrDefault(parameters, "k_default",
				-1.);//Double.valueOf((String) parameters.getOrDefault("k_default", "-1."));
		if (this.kDefault == -1) {
			this.kDefault = null;
		}

		this.minutesChangeK = getParameterIntOrDefault(parameters, "minutes_change_k", 1);
	}

	public void init() {
		super.init();
		if (this.midpricesQueue == null) {
			//dont delete it if exists
			logger.info("creating midpricesQueue of len {}", this.windowTick);
			this.midpricesQueue = EvictingQueue.create(this.windowTick);
		}

		this.counterTradesPerMinute = EvictingQueue.create(60);//last element in the -1 index
		this.counterBuyTradesPerMinute = EvictingQueue.create(60);//last element in the -1 index
		this.counterSellTradesPerMinute = EvictingQueue.create(60);//last element in the -1 index
		sideActive = new ConcurrentHashMap<>();

	}

	protected void setMidPricesQueue(int windowTick) {
		//For RL
		this.midpricesQueue = EvictingQueue.create(windowTick);
	}


	@Override public void setParameter(String name, Object value) {
		super.setParameter(name, value);
	}

	@Override public String printAlgo() {
		return String
				.format("%s  \n\triskAversion=%.3f\n\tquantity=%.3f\n\twindowTick=%d\n\tfirstHourOperatingIncluded=%d\n\tlastHourOperatingIncluded=%d\n\tminutesChangeK=%d",
						algorithmInfo, riskAversion, quantity, windowTick, firstHourOperatingIncluded,
						lastHourOperatingIncluded, minutesChangeK);
	}

	@Override public boolean onTradeUpdate(Trade trade) {
		if (!super.onTradeUpdate(trade)) {
			return false;
		}
		if (counterStartingMinuteMs == 0) {
			counterStartingMinuteMs = getCurrentTimestamp();
		}
		boolean minuteHasPassed = getCurrentTimestamp() - counterStartingMinuteMs > 60 * 1000;
		if (minuteHasPassed) {
			counterTradesPerMinute.add(counterTrades);//counter per minute
			counterBuyTradesPerMinute.add(counterBuyTrades);//counter per minute
			counterSellTradesPerMinute.add(counterSellTrades);//counter per minute
			counterStartingMinuteMs = getCurrentTimestamp();
			counterTrades = 0L;
			counterBuyTrades = 0L;
			counterSellTrades = 0L;

		} else {
			InstrumentManager instrumentManager = getInstrumentManager(trade.getInstrument());
			Depth lastDepth = instrumentManager.getLastDepth();

			if (lastDepth != null && lastDepth.isDepthFilled()) {
				if (trade.getPrice() < lastDepth.getBestBid()) {
					//buy
					counterBuyTrades++;
				}
				if (trade.getPrice() > lastDepth.getBestAsk()) {
					//buy
					counterSellTrades++;
				}

			}
			counterTrades++;
		}
		return true;

	}

	@Override public boolean onDepthUpdate(Depth depth) {
		if (!super.onDepthUpdate(depth)) {
			return false;
		}

		checkSideDisable(getCurrentTimestamp());
		if (!depth.isDepthFilled()) {
			//			logger.warn("Depth received incomplete! {}-> disable", depth.getInstrument());
			this.stop();
			return false;
		} else {
			this.start();
		}

		double midPrice = depth.getMidPrice();
		double currentSpread = depth.getSpread();
		this.midpricesQueue.add(midPrice);
		Double varianceMidPrice = getVarianceMidPrice();
		if (varianceMidPrice == null || !Double.isFinite(varianceMidPrice)) {
			return false;
		}
		this.varianceMidPrice = varianceMidPrice;

		double T_t = getTt();
		double position = (getPosition(this.instrument) - targetPosition) * positionMultiplier;
		double reservePrice = midPrice - (position * this.riskAversion * varianceMidPrice * T_t);
		//		double numberOfTimesOut = (Math.abs(reservePrice - midPrice)) / midPrice;
		//		if (numberOfTimesOut > 0.5) {
		//			logger.warn("wrong calculation of reservePrice > 400 pct midprice!=> setting to midprice {}", midPrice);
		//			reservePrice = midPrice;
		//		}
		double kTotal = calculateK(this.counterTradesPerMinute);
		if (!Double.isFinite(kTotal) || kTotal == 0) {
			return false;
		}
		//each side
		double kBuy = calculateK(this.counterBuyTradesPerMinute);
		if (!Double.isFinite(kBuy) || kBuy == 0) {
			return false;
		}

		double kSell = calculateK(this.counterSellTradesPerMinute);
		if (!Double.isFinite(kSell) || kSell == 0) {
			return false;
		}

		//		double spreadBid_ =
		//				(riskAversion * varianceMidPrice * T_t) + (2 / riskAversion) * Math.log(1 + (riskAversion / kTotal));

		double spreadBid_ =
				(riskAversion * varianceMidPrice * T_t) + (2 / riskAversion) * Math.log(1 + (riskAversion / kBuy));

		double spreadBid = spreadBid_ * currentSpread * spreadMultiplier;//alridge
		//		double spreadAsk_ =
		//				(riskAversion * varianceMidPrice * T_t) + (2 / riskAversion) * Math.log(1 + (riskAversion / kTotal));
		double spreadAsk_ =
				(riskAversion * varianceMidPrice * T_t) + (2 / riskAversion) * Math.log(1 + (riskAversion / kSell));
		double spreadAsk = spreadAsk_ * currentSpread * spreadMultiplier;

		try {

			double askPrice = (reservePrice + spreadAsk) * (1 + skewPricePct);
			askPrice = Precision.round(askPrice, instrument.getNumberDecimalsPrice());

			double bidPrice = (reservePrice - spreadBid) * (1 + skewPricePct);
			bidPrice = Precision.round(bidPrice, instrument.getNumberDecimalsPrice());
			if (!Double.isFinite(askPrice) || !Double.isFinite(bidPrice)) {
				logger.warn("wrong calculation ask-bid");
				return false;
			}
			//Check not crossing the mid price!
			askPrice = Math.max(askPrice, depth.getMidPrice() + instrument.getPriceTick());
			bidPrice = Math.min(bidPrice, depth.getMidPrice() - instrument.getPriceTick());

			//			Check worst price
			double maxAskPrice = depth.getMidPrice() + MAX_TICKS_MIDPRICE_PRICE_DEV * instrument.getPriceTick();
			askPrice = Math.min(askPrice, maxAskPrice);
			double minBidPrice = depth.getMidPrice() - MAX_TICKS_MIDPRICE_PRICE_DEV * instrument.getPriceTick();
			bidPrice = Math.max(bidPrice, minBidPrice);

			//create quote request
			QuoteRequest quoteRequest = createQuoteRequest(this.instrument);
			quoteRequest.setQuoteRequestAction(QuoteRequestAction.On);
			quoteRequest.setBidPrice(bidPrice);
			quoteRequest.setAskPrice(askPrice);
			quoteRequest.setBidQuantity(this.quantity);
			quoteRequest.setAskQuantity(this.quantity);

			//remove side disable!
			for (Map.Entry<Verb, Boolean> entry : sideActive.entrySet()) {
				boolean isActive = entry.getValue();
				Verb verb = entry.getKey();
				if (!isActive) {
					if (verb.equals(Verb.Buy)) {
						quoteRequest.setBidQuantity(0.);
					}
					if (verb.equals(Verb.Sell)) {
						quoteRequest.setAskQuantity(0.);
					}
				}
			}

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

	private void checkSideDisable(long currentTimestamp) {
		if (!autoEnableSideTime) {
			return;
		}
		Map<Verb, Boolean> sideActiveOutput = new ConcurrentHashMap<>(sideActive);
		for (Map.Entry<Verb, Boolean> entry : sideActive.entrySet()) {
			Verb verb = entry.getKey();
			Boolean isActive = entry.getValue();
			long disableTime = 0L;
			if (!isActive) {
				try {
					disableTime = getInstrumentManager().getLastTradeTimestamp().get(verb);
				} catch (NullPointerException e) {
					//in case of nullpointer -> enable it again
				}

				long elapsedTimeMs = currentTimestamp - disableTime;
				if (elapsedTimeMs > stopTradeSideMs) {
					//enable again
					logger.info("enable side {} at {}", verb, getCurrentTime());
					sideActiveOutput.put(verb, true);
				}

			}

		}
		this.sideActive = sideActiveOutput;

	}

	/***
	 * from High Freq trading by Irene Alridge , page 139
	 *         k_bid = λb/delta(λb)  / delta(λb)=(λb-λb-1)/λb-1
	 *
	 *
	 * @return K_total
	 */
	private double calculateK(Queue<Long> counterTradesPerMinuteInput) {
		if (kDefault != null) {
			return kDefault;
		}
		if (counterTradesPerMinuteInput.size() < minutesChangeK + 1) {
			return 0.;
		}
		Long[] counterTradesPerMinute = new Long[counterTradesPerMinuteInput.size()];
		counterTradesPerMinute = counterTradesPerMinuteInput.toArray(counterTradesPerMinute);

		double lastMinuteTrades = Double.valueOf(counterTradesPerMinute[counterTradesPerMinuteInput.size() - 1]);
		double initialMinuteTrades = Double
				.valueOf(counterTradesPerMinute[counterTradesPerMinuteInput.size() - (minutesChangeK + 1)]);

		//		 k_total = count_total / (
		//                    (count_total - count_total_before) / count_total_before
		//                )
		double denominator = (lastMinuteTrades - initialMinuteTrades) / initialMinuteTrades;
		return lastMinuteTrades / denominator;
	}

	private Double getVarianceMidPrice() {

		if (midpricesQueue.size() < windowTick) {
			return null;
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
			logger.error("error calculating variance on {} windows tick with size {}-> return last varianceMidPrice",
					windowTick, midpricesQueue.size());
			return this.varianceMidPrice;
		}
		double sum = 0.;
		for (int i = 0; i < windowTick; i++) {
			sum += midPricesArr[i];
		}
		double mean = sum / (double) windowTick;
		double sqDiff = 0;
		for (int i = 0; i < windowTick; i++) {
			sqDiff += (midPricesArr[i] - mean) * (midPricesArr[i] - mean);
		}
		double var = (double) sqDiff / windowTick;
		return var;
	}

	private double getTt() {
		int hour = getCurrentTimeHour();
		double num = Math.max(lastHourOperatingIncluded - hour, 0);
		double den = lastHourOperatingIncluded - firstHourOperatingIncluded;
		return num / den;
	}

	@Override protected void sendOrderRequest(OrderRequest orderRequest) throws LambdaTradingException {
		//		logger.info("sendOrderRequest {} {}", orderRequest.getOrderRequestAction(), orderRequest.getClientOrderId());
		super.sendOrderRequest(orderRequest);

	}

	@Override public boolean onExecutionReportUpdate(ExecutionReport executionReport) {
		boolean output = super.onExecutionReportUpdate(executionReport);
		if (executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled)
				|| executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.PartialFilled)) {
			logger.debug("trade arrived");
			try {
				getQuoteManager(executionReport.getInstrument()).unquoteSide(executionReport.getVerb());
			} catch (LambdaTradingException e) {
				logger.error("cant unquote verb {} => cancel manual", executionReport.getVerb(), e);
				//cancel all this side active
				cancelAllVerb(instrument, executionReport.getVerb());
			}
			//disable this side
			if (DISABLE_ON_HIT) {
				autoEnableSideTime = true;//need to be enable to autodisable it in time
				logger.info("disable {} side at {}", executionReport.getVerb(), getCurrentTime());
				sideActive.put(executionReport.getVerb(), false);
			}


		}
		return output;
	}
}
