package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.Configuration;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn.CandleManager;
import com.lambda.investing.market_data_connector.MarketDataListener;
import com.lambda.investing.market_data_connector.Statistics;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.candle.Candle;
import com.lambda.investing.model.exception.LambdaTradingException;
import com.lambda.investing.model.market_data.Depth;
import com.lambda.investing.model.market_data.Trade;
import com.lambda.investing.model.messaging.Command;
import com.lambda.investing.model.trading.*;
import com.lambda.investing.trading_engine_connector.ExecutionReportListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import javax.annotation.PostConstruct;
import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Algorithm implements MarketDataListener, ExecutionReportListener {

	private static String SEPARATOR_ARRAY_PARAMETERS = ",";
	public static int LOG_LEVEL = LogLevels.ALL_ITERATION_LOG.ordinal();//0 is s
	private static String REJECTION_NOT_FOUND = "not found for";
	private static Long STATISTICS_PRINT_SECONDS = 60L;
	private List<AlgorithmObserver> algorithmObservers;
	private static String BASE_PATH_OUTPUT = Configuration.OUTPUT_PATH;
	private static Integer FIRST_HOUR_DEFAULT = -1;
	private static Integer LAST_HOUR_DEFAULT = 25;

	private static final String SEND_STATS = "->";
	private static final String RECEIVE_STATS = "<-";

	protected Integer firstHourOperatingIncluded = FIRST_HOUR_DEFAULT;//starting
	protected Integer lastHourOperatingIncluded = LAST_HOUR_DEFAULT;//stopping

	private AtomicInteger depthReceived = new AtomicInteger(0);
	private AtomicInteger tradeReceived = new AtomicInteger(0);

	protected Logger logger = LogManager.getLogger(Algorithm.class);

	private AlgorithmConnectorConfiguration algorithmConnectorConfiguration; //must be private because send orders must pass from here
	protected String algorithmInfo;

	public String getAlgorithmInfo() {
		return algorithmInfo;
	}

	private Map<String, InstrumentManager> instrumentToManager;

	public QuoteManager getQuoteManager(String instrumentPk) {

		QuoteManager quoteManager = instrumentQuoteManagerMap.get(instrumentPk);
		if (quoteManager == null) {
			quoteManager = new QuoteManager(this, Instrument.getInstrument(instrumentPk));
			instrumentQuoteManagerMap.put(instrumentPk, quoteManager);
		}
		return instrumentQuoteManagerMap.get(instrumentPk);
	}

	private Map<String, Object> defaultParameters;
	protected Map<String, Object> parameters;

	protected Statistics statistics;

	public PnlSnapshot getLastPnlSnapshot(String instrumentPk) {
		return portfolioManager.getLastPnlSnapshot(instrumentPk);
	}

	public void addCurrentCustomColumn(String instrumentPk, String key, Double value) {
		portfolioManager.addCurrentCustomColumn(instrumentPk, key, value);
	}

	protected PortfolioManager portfolioManager;
	protected AlgorithmNotifier algorithmNotifier;

	protected boolean isBacktest = false;
	protected TimeServiceIfc timeService;
	private AlgorithmState algorithmState = AlgorithmState.NOT_INITIALIZED;

	public AlgorithmState getAlgorithmState() {
		return algorithmState;
	}

	public Map<String, ExecutionReport> clientOrderIdLastCompletelyFillReceived;
	private static int HISTORICAL_TRADES_SAVE = 50;
	private boolean plotStopHistorical = true;
	private boolean exitOnStop = true;
	private int lastCurrentDay = 0;
	//	private List<String> pendingToRemoveClientOrderId;
	private Map<String, QuoteManager> instrumentQuoteManagerMap;

	private Map<String, OrderRequest> clientOrderIdToCancelWhenActive;
	private CandleFromTickUpdater candleFromTickUpdater;
	public static final Object EXECUTION_REPORT_LOCK = new Object();

	public Algorithm(AlgorithmConnectorConfiguration algorithmConnectorConfiguration, String algorithmInfo,
			Map<String, Object> parameters) {
		constructorForAbstract(algorithmConnectorConfiguration, algorithmInfo, parameters);
	}

	//for backtesting
	public Algorithm(String algorithmInfo, Map<String, Object> parameters) {
		constructorForAbstract(null, algorithmInfo, parameters);
	}

	protected double getParameterDoubleOrDefault(Map<String, Object> parameters, String key, double defaultValue) {
		String value = (String) parameters.getOrDefault(key, String.valueOf(defaultValue));

		try {
			return Double.valueOf(value);
		} catch (Exception e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}

	protected double getParameterDouble(Map<String, Object> parameters, String key) {
		String value = (String) parameters.get(key);

		try {
			return Double.valueOf(value);
		} catch (Exception e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}

	protected int getParameterIntOrDefault(Map<String, Object> parameters, String key, int defaultValue) {
		String value = (String) parameters.getOrDefault(key, String.valueOf(defaultValue));
		try {
			return (int) Math.round(Double.valueOf(value));
		} catch (Exception e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}
	protected int getParameterInt(Map<String, Object> parameters, String key) {

		String value = (String) parameters.get(key);
		try {
			return (int) Math.round(Double.valueOf(value));
		} catch (Exception e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}

	}

	protected String getParameterString(Map<String, Object> parameters, String key) {
		return (String) parameters.get(key);
	}

	protected String[] getParameterArrayString(Map<String, Object> parameters, String key) {
		String value = (String) parameters.get(key);
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		try {
			String[] output = null;
			if (!value.contains(SEPARATOR_ARRAY_PARAMETERS)) {
				String valueIn = getParameterString(parameters, key);
				output = new String[] { valueIn };
			} else {
				String[] splitted = value.split(SEPARATOR_ARRAY_PARAMETERS);
				output = new String[splitted.length];
				for (int index = 0; index < splitted.length; index++) {
					output[index] = splitted[index];
				}
			}
			return output;
		} catch (NullPointerException e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}


	protected double[] getParameterArrayDouble(Map<String, Object> parameters, String key) {
		String value = (String) parameters.get(key);
		try {
			double[] output = null;
			if (!value.contains(SEPARATOR_ARRAY_PARAMETERS)) {
				double valueIn = getParameterDouble(parameters, key);
				output = new double[] { valueIn };
			} else {
				String[] splitted = value.split(SEPARATOR_ARRAY_PARAMETERS);
				output = new double[splitted.length];
				for (int index = 0; index < splitted.length; index++) {
					output[index] = Double.valueOf(splitted[index]);
				}
			}
			return output;
		} catch (NullPointerException e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}

	protected int[] getParameterArrayInt(Map<String, Object> parameters, String key) {
		String value = (String) parameters.get(key);
		try {
			int[] output = null;
			if (!value.contains(SEPARATOR_ARRAY_PARAMETERS)) {
				int valueIn = getParameterInt(parameters, key);
				output = new int[] { valueIn };
			} else {
				String[] splitted = value.split(SEPARATOR_ARRAY_PARAMETERS);
				output = new int[splitted.length];
				for (int index = 0; index < splitted.length; index++) {
					output[index] = (int) Math.round(Double.valueOf(splitted[index]));
				}
			}
			return output;
		} catch (NullPointerException e) {
			System.err.println(String.format("wrong parameter %s with value %s", key, value));
			throw e;
		}
	}

	public void constructorForAbstract(AlgorithmConnectorConfiguration algorithmConnectorConfiguration,
			String algorithmInfo, Map<String, Object> parameters) {
		candleFromTickUpdater= new CandleFromTickUpdater(this);
		algorithmObservers = new ArrayList<>();

		if (algorithmConnectorConfiguration != null) {
			isBacktest = false;
			this.algorithmConnectorConfiguration = algorithmConnectorConfiguration;
			timeService = new TimeService("UTC");
		} else {
			logger.info("BACKTEST detected in {} -> Backtest TimeService", algorithmInfo);
			isBacktest = true;
			timeService = new BacktestTimeService("UTC");
		}

		if (!isBacktest) {
			this.statistics = new Statistics(algorithmInfo, STATISTICS_PRINT_SECONDS * 1000);
		}
		this.algorithmInfo = algorithmInfo;
		this.parameters = parameters;
		this.defaultParameters = new ConcurrentHashMap<>(parameters);
		instrumentQuoteManagerMap = new ConcurrentHashMap<>();

		this.instrumentToManager = new ConcurrentHashMap<>();

		this.portfolioManager = new PortfolioManager(this);
		clientOrderIdLastCompletelyFillReceived = Collections
				.synchronizedMap(new LinkedHashMap<String, ExecutionReport>(HISTORICAL_TRADES_SAVE) {

					@Override protected boolean removeEldestEntry(Map.Entry<String, ExecutionReport> entry) {
						return size() > HISTORICAL_TRADES_SAVE;
					}
				});
		algorithmNotifier = new AlgorithmNotifier(this, 3);
		reset();

	}

	public void setPlotStopHistorical(boolean plotStopHistorical) {
		this.plotStopHistorical = plotStopHistorical;
	}

	public void setExitOnStop(boolean exitOnStop) {
		this.exitOnStop = exitOnStop;
	}

	public InstrumentManager getInstrumentManager(String instrumentPk) {
		InstrumentManager instrumentManager = instrumentToManager.get(instrumentPk);
		if (instrumentManager == null) {
			instrumentManager = new InstrumentManager(Instrument.getInstrument(instrumentPk), isBacktest);
			instrumentToManager.put(instrumentPk, instrumentManager);
		}
		return instrumentToManager.get(instrumentPk);
	}

	public Map<String, ExecutionReport> getActiveOrders(Instrument instrument) {
		InstrumentManager instrumentManager = getInstrumentManager(instrument.getPrimaryKey());
		return instrumentManager.getAllActiveOrders();
	}

	public Map<String, OrderRequest> getRequestOrders(Instrument instrument) {
		InstrumentManager instrumentManager = getInstrumentManager(instrument.getPrimaryKey());
		return instrumentManager.getAllRequestOrders();
	}

	public void setAllRequestOrders(Instrument instrument, Map<String, OrderRequest> requestOrders) {
		InstrumentManager instrumentManager = getInstrumentManager(instrument.getPrimaryKey());
		instrumentManager.setAllRequestOrders(requestOrders);
	}

	public void register(AlgorithmObserver algorithmObserver) {
		algorithmObservers.add(algorithmObserver);
	}

	public void deregister(AlgorithmObserver algorithmObserver) {
		algorithmObservers.remove(algorithmObserver);
	}

	public List<AlgorithmObserver> getAlgorithmObservers() {
		return algorithmObservers;
	}

	public void setAlgorithmConnectorConfiguration(AlgorithmConnectorConfiguration algorithmConnectorConfiguration) {
		this.algorithmConnectorConfiguration = algorithmConnectorConfiguration;
	}

	//Parameter settings
	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
		this.firstHourOperatingIncluded = getParameterIntOrDefault(parameters, "first_hour",
				FIRST_HOUR_DEFAULT);//UTC time 6 -9
		this.lastHourOperatingIncluded = getParameterIntOrDefault(parameters, "last_hour",
				LAST_HOUR_DEFAULT);//UTC  18-21

		if (this.firstHourOperatingIncluded != FIRST_HOUR_DEFAULT
				|| this.lastHourOperatingIncluded != LAST_HOUR_DEFAULT) {

			logger.info("Current time {} in utc is {}", new Date(), Instant.now());
			logger.info("start UTC hour at {} and finished on {} ,both included", this.firstHourOperatingIncluded,
					this.lastHourOperatingIncluded);

		}
		algorithmNotifier.notifyObserversOnUpdateParams(this.parameters);

	}

	public void setParameter(String name, Object value) {
		this.parameters.put(name, value);
		algorithmNotifier.notifyObserversOnUpdateParams(this.parameters);
	}

	public abstract String printAlgo();

	@PostConstruct public void init() {
		if (algorithmState.getNumber() < 0) {
			algorithmState = AlgorithmState.INITIALIZING;
			logger.info("[{}]Initializing algorithm {}", getCurrentTime(), algorithmInfo);
			if (algorithmConnectorConfiguration == null) {
				logger.error("cant initialize {} without AlgorithmConnectorConfiguration set", algorithmInfo);
			}
			this.algorithmConnectorConfiguration.getTradingEngineConnector().register(this.algorithmInfo, this);
			this.algorithmConnectorConfiguration.getMarketDataProvider().register(this);
			reset();
			algorithmState = AlgorithmState.INITIALIZED;
			logger.info("[{}]initialized  {}\n{}", getCurrentTime(), algorithmInfo);

		} else {
			logger.warn("trying to init already initialized {}", algorithmInfo);
		}
	}

	//PostContruct and destroy resset
	public void start() {
		if (algorithmState.equals(AlgorithmState.INITIALIZED) || algorithmState.equals(AlgorithmState.STOPPED)) {
			algorithmState = AlgorithmState.STARTING;
			logger.info("[{}]Starting algorithm {}", getCurrentTime(), algorithmInfo);

			algorithmState = AlgorithmState.STARTED;

		}
	}

	public void stop() {
		if (algorithmState.equals(AlgorithmState.STARTED)) {
			algorithmState = AlgorithmState.STOPPING;

			logger.info("[{}]Stopping algorithm {}", getCurrentTime(), algorithmInfo);
			algorithmState = AlgorithmState.STOPPED;
			//To be sure everything has finished
			try {
				if (!isBacktest) {
					timeService.sleepMs(200);
				}
			} catch (InterruptedException e) {
				logger.error("cant sleep on stopped algorithm", e);
			}
			for (InstrumentManager instrumentManager : instrumentToManager.values()) {
				Instrument instrument = instrumentManager.getInstrument();
				try {
					getQuoteManager(instrument.getPrimaryKey()).unquote();
				} catch (LambdaTradingException e) {
					logger.error("cant unquote instrument {} on stop algorithm", instrument.getPrimaryKey(), e);
				}
				cancelAll(instrument);
			}
		}
	}

	public Depth getLastDepth(Instrument instrument) {
		return getInstrumentManager(instrument.getPrimaryKey()).getLastDepth();
	}

	public void reset() {
		logger.info("[{}]Reset algorithm {}", getCurrentTime(), algorithmInfo);
		//		this.portfolioManager.reset();//will reset trades!
		clientOrderIdLastCompletelyFillReceived.clear();
		for (InstrumentManager instrumentManager : instrumentToManager.values()) {
			instrumentManager.reset();
		}
		clientOrderIdToCancelWhenActive = new ConcurrentHashMap<>();
		for (QuoteManager quoteManager : instrumentQuoteManagerMap.values()) {
			quoteManager.reset();
		}
		this.parameters = new ConcurrentHashMap<>(defaultParameters);
		depthReceived = new AtomicInteger(0);
		tradeReceived = new AtomicInteger(0);
		//		pendingToRemoveClientOrderId = new ArrayList<>();
	}

	//trading

	protected String generateClientOrderId() {
		return UUID.randomUUID().toString();
	}

	public OrderRequest createCancel(Instrument instrument, String origClientOrderId) {
		OrderRequest cancelOrderRequest = new OrderRequest();
		cancelOrderRequest.setOrderRequestAction(OrderRequestAction.Cancel);
		cancelOrderRequest.setOrigClientOrderId(origClientOrderId);
		cancelOrderRequest.setAlgorithmInfo(algorithmInfo);
		cancelOrderRequest.setClientOrderId(generateClientOrderId());
		cancelOrderRequest.setInstrument(instrument.getPrimaryKey());
		cancelOrderRequest.setTimestampCreation(getCurrentTimestamp());
		return cancelOrderRequest;
	}

	public void cancelAllVerb(Instrument instrument, Verb verb) {
		InstrumentManager instrumentManager = getInstrumentManager(instrument.getPrimaryKey());
		Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();

		if (instrumentActiveOrders.size() > 0 && LOG_LEVEL > LogLevels.DISABLE.ordinal()) {
			logger.info("cancelAll verb {}  {} active orders {}", verb, instrument, instrumentActiveOrders.size());
		}
		for (String clientOrderId : instrumentActiveOrders.keySet()) {
			ExecutionReport executionReport = instrumentActiveOrders.get(clientOrderId);
			if (!executionReport.getVerb().equals(verb)) {
				continue;
			}

			OrderRequest cancelOrderRequest = createCancel(instrument, clientOrderId);
			this.algorithmConnectorConfiguration.getTradingEngineConnector().orderRequest(cancelOrderRequest);
		}

		//save requested orders to cancel after active received
		Map<String, OrderRequest> requestOrders = instrumentManager.getAllRequestOrders();
		for (String clientOrderId : requestOrders.keySet()) {
			OrderRequest orderRequest = requestOrders.get(clientOrderId);
			if (orderRequest.getVerb().equals(verb)) {
				clientOrderIdToCancelWhenActive.put(clientOrderId, orderRequest);
			}
		}

	}

	public void cancelAll(Instrument instrument) {
		InstrumentManager instrumentManager = getInstrumentManager(instrument.getPrimaryKey());
		Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();

		if (instrumentActiveOrders.size() > 0 && LOG_LEVEL > LogLevels.DISABLE.ordinal()) {
			logger.info("cancelAll {} active orders {}", instrument, instrumentActiveOrders.size());
		}
		for (String clientOrderId : instrumentActiveOrders.keySet()) {
			OrderRequest cancelOrderRequest = createCancel(instrument, clientOrderId);
			this.algorithmConnectorConfiguration.getTradingEngineConnector().orderRequest(cancelOrderRequest);
		}

		//save requested orders to cancel after active received
		Map<String, OrderRequest> requestOrders = instrumentManager.getAllRequestOrders();
		for (String clientOrderId : requestOrders.keySet()) {
			clientOrderIdToCancelWhenActive.put(clientOrderId, requestOrders.get(clientOrderId));
		}

	}

	public QuoteRequest createQuoteRequest(Instrument instrument) {
		QuoteRequest quoteRequest = new QuoteRequest();
		quoteRequest.setAlgorithmInfo(algorithmInfo);
		quoteRequest.setInstrument(instrument);
		return quoteRequest;
	}

	protected boolean inOperationalTime() {
		int hour = getCurrentTimeHour();
		if (hour >= firstHourOperatingIncluded && hour <= lastHourOperatingIncluded) {
			return true;
		}
		return false;
	}

	protected boolean checkOperationalTime() {
		if (inOperationalTime()) {
			start();
			return true;
		} else {
			stop();
		}
		if (lastCurrentDay == 0) {
			lastCurrentDay = getCurrentTimeDay();
		}
		if (lastCurrentDay != getCurrentTimeDay()) {
			logger.info("change of day detected-> reset");
			//			System.out.println("new day detected " + algorithmInfo);
			lastCurrentDay = getCurrentTimeDay();
			reset();
		}

		return false;
	}

	private void updateAllActiveOrders(ExecutionReport executionReport) {
		boolean isActive =
				executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.Active) || executionReport
						.getExecutionReportStatus().equals(ExecutionReportStatus.PartialFilled);
		boolean isInactive =
				executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.Cancelled) || executionReport
						.getExecutionReportStatus().equals(ExecutionReportStatus.Rejected) || executionReport
						.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled);

		boolean isCancelRejected = executionReport.getExecutionReportStatus()
				.equals(ExecutionReportStatus.CancelRejected);
		//todo search on active to delete in case

		boolean isFilled = executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.PartialFilled)
				|| executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled);

		String instrumentPk = executionReport.getInstrument();
		InstrumentManager instrumentManager = getInstrumentManager(instrumentPk);

		Queue<String> tradesInstrument = instrumentManager.getCfTradesReceived();

		//remove from requestOrders
		Map<String, OrderRequest> instrumentSendOrders = instrumentManager.getAllRequestOrders();
		//		if (!instrumentSendOrders.containsKey(executionReport.getClientOrderId())) {
		//			logger.warn("received ER very fast {}    {}", executionReport.getClientOrderId(),executionReport.getExecutionReportStatus());
		//			pendingToRemoveClientOrderId.add(executionReport.getClientOrderId());
		//		}
		instrumentSendOrders.remove(executionReport.getClientOrderId());
		setAllRequestOrders(instrumentManager.getInstrument(), instrumentSendOrders);

		if (isActive) {
			boolean wasACfTrade = tradesInstrument.contains(executionReport.getClientOrderId());
			if (!wasACfTrade) {

				Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();

				instrumentActiveOrders.put(executionReport.getClientOrderId(), executionReport);
				//remove in case of modify!
				if (executionReport.getOrigClientOrderId() != null) {
					instrumentActiveOrders.remove(executionReport.getOrigClientOrderId());
				}
				//update it
				instrumentManager.setAllActiveOrders(instrumentActiveOrders);
				if (LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
					logger.debug("ER {} received active  {} ", executionReport.getClientOrderId(),
							executionReport.getVerb());
				}

				if (clientOrderIdToCancelWhenActive.containsKey(executionReport.getClientOrderId())) {
					if (LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
						logger.debug("ER {} detected to be canceled", executionReport.getClientOrderId());
					}
					OrderRequest cancelRequest = createCancel(instrumentManager.getInstrument(),
							executionReport.getClientOrderId());
					try {
						sendOrderRequest(cancelRequest);
						clientOrderIdToCancelWhenActive.remove(executionReport.getClientOrderId());
					} catch (LambdaTradingException e) {
						logger.error("cant cancel waiting order {}", executionReport.getClientOrderId());
					}
				}

			} else {
				if (LOG_LEVEL > LogLevels.ALL_ITERATION_LOG.ordinal()) {
					logger.warn("received active {} of previous Cf trade => ignore",
							executionReport.getClientOrderId());
				}
			}
		}

		if (isInactive) {
			if (executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled)) {
				tradesInstrument.add(executionReport.getClientOrderId());
				instrumentManager.setCfTradesReceived(tradesInstrument);
			}

			Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();
			instrumentActiveOrders.remove(executionReport.getClientOrderId());
			//just in case
			if (executionReport.getOrigClientOrderId() != null) {
				instrumentActiveOrders.remove(executionReport.getOrigClientOrderId());
			}
			instrumentManager.setAllActiveOrders(instrumentActiveOrders);
			if (LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
				logger.debug("ER {} received inactive ", executionReport.getClientOrderId());
			}

		}

		if (isCancelRejected) {
			Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();
			if (executionReport.getRejectReason().contains(REJECTION_NOT_FOUND) && instrumentActiveOrders
					.containsKey(executionReport.getOrigClientOrderId())) {
				//remove it from active
				instrumentActiveOrders.remove(executionReport.getOrigClientOrderId());
				instrumentActiveOrders.remove(executionReport.getClientOrderId());//just in case
				instrumentManager.setAllActiveOrders(instrumentActiveOrders);
			}
			if (LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
				logger.debug("ER {} cancel rejected on {} ", executionReport.getClientOrderId(),
						executionReport.getOrigClientOrderId());
			}

		}

		if (isFilled) {
			if (LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
				logger.debug("ER filled {} {}    {}@{} ", executionReport.getClientOrderId(),
						executionReport.getExecutionReportStatus(), executionReport.getLastQuantity(),
						executionReport.getPrice());
			}

			Map<Verb, Long> currentLastTradeTimestamp = instrumentManager.getLastTradeTimestamp();
			currentLastTradeTimestamp.put(executionReport.getVerb(), getCurrentTimestamp());
			instrumentManager.setLastTradeTimestamp(currentLastTradeTimestamp);
		}

	}

	private OrderRequest checkOrderRequest(OrderRequest orderRequest) throws LambdaTradingException {
		String instrumentPk = orderRequest.getInstrument();
		InstrumentManager instrumentManager = getInstrumentManager(instrumentPk);

		//Check order request
		if (orderRequest.getClientOrderId() == null) {
			orderRequest.setClientOrderId(generateClientOrderId());
		}

		if (orderRequest.getAlgorithmInfo() == null) {
			orderRequest.setAlgorithmInfo(algorithmInfo);
		}

		if (orderRequest.getOrderRequestAction() == null) {
			throw new LambdaTradingException("OrderRequest without action");
		}

		if (orderRequest.getOrderRequestAction().equals(OrderRequestAction.Send) || orderRequest.getOrderRequestAction()
				.equals(OrderRequestAction.Modify)) {
			if (orderRequest.getOrderType() == null) {
				throw new LambdaTradingException(String.format("OrderRequest %s without ordertype",
						orderRequest.getOrderRequestAction().name()));
			}

			if (orderRequest.getVerb() == null) {
				throw new LambdaTradingException(
						String.format("OrderRequest %s without verb", orderRequest.getOrderRequestAction().name()));
			}

		}

		boolean needOrigClientOrdId =
				orderRequest.getOrderRequestAction().equals(OrderRequestAction.Modify) || orderRequest
						.getOrderRequestAction().equals(OrderRequestAction.Cancel);

		if (needOrigClientOrdId && orderRequest.getOrigClientOrderId() == null) {
			String message = String
					.format("%s is a %s need a OrigClientOrderId !=null in %s", orderRequest.getClientOrderId(),
							orderRequest.getOrderRequestAction(), algorithmInfo);
			throw new LambdaTradingException(message);
		}

		if (needOrigClientOrdId && orderRequest.getOrigClientOrderId() != null) {
			String origClientOrderId = orderRequest.getOrigClientOrderId();
			Map<String, ExecutionReport> instrumentActiveOrders = instrumentManager.getAllActiveOrders();
			boolean isConfirmed = instrumentActiveOrders.containsKey(origClientOrderId);
			if (!isConfirmed) {
				String message = String
						.format("%s is a %s need a OrigClientOrderId[%s] active in %s", orderRequest.getClientOrderId(),
								orderRequest.getOrderRequestAction(), orderRequest.getOrigClientOrderId(),
								algorithmInfo);
				throw new LambdaTradingException(message);
			}

		}

		return orderRequest;
	}

	public void sendQuoteRequest(QuoteRequest quoteRequest) throws LambdaTradingException {
		if (quoteRequest.getQuoteRequestAction().equals(QuoteRequestAction.On) && !getAlgorithmState()
				.equals(AlgorithmState.STARTED)) {
			throw new LambdaTradingException("cant quote with algo not started");
		}
		QuoteManager quoteManager = getQuoteManager(quoteRequest.getInstrument().getPrimaryKey());
		quoteManager.quoteRequest(quoteRequest);
	}

	private void addStatistics(String topic) {
		if (this.statistics != null) {
			this.statistics.addStatistics(topic);
		}
	}

	protected void sendOrderRequest(OrderRequest orderRequest) throws LambdaTradingException {
		if (!algorithmState.equals(AlgorithmState.STARTED) && !orderRequest.getOrderRequestAction()
				.equals(OrderRequestAction.Cancel)) {
			//cancel can be sent_
			throw new LambdaTradingException(
					"cant send new/modify order with algo " + this.algorithmInfo + " not started");
		}

		String instrumentPk = orderRequest.getInstrument();
		InstrumentManager instrumentManager = getInstrumentManager(instrumentPk);
		orderRequest = checkOrderRequest(orderRequest);
		orderRequest.setTimestampCreation(getCurrentTimestamp());

		//updating the OrderRequestMap before sending
		Map<String, OrderRequest> instrumentSendOrders = instrumentManager.getAllRequestOrders();
		//		if(pendingToRemoveClientOrderId.contains(orderRequest.getClientOrderId())){
		//			pendingToRemoveClientOrderId.remove(orderRequest.getClientOrderId());
		//		}else {
		instrumentSendOrders.put(orderRequest.getClientOrderId(), orderRequest);
		setAllRequestOrders(instrumentManager.getInstrument(), instrumentSendOrders);
		//		}

		this.algorithmConnectorConfiguration.getTradingEngineConnector().orderRequest(orderRequest);

		addStatistics("orderRequest." + orderRequest.getOrderRequestAction().name() + " " + SEND_STATS);
		algorithmNotifier.notifyObserversOnOrderRequest(orderRequest);
	}

	public void onUpdateCandle(Candle candle){}

	@Override public boolean onDepthUpdate(Depth depth) {
		long timestamp = depth.getTimestamp();
		candleFromTickUpdater.onDepthUpdate(depth);
		if (timestamp != 0 && isBacktest) {
			timeService.setCurrentTimestamp(timestamp);
		}
		if (!isBacktest) {
			timeService.setCurrentTimestamp(new Date().getTime());
		}

		if (!checkOperationalTime()) {
			return false;
		}
		//check depth
		checkDepth(depth);

		//update cache
		portfolioManager.updateDepth(depth);
		InstrumentManager instrumentManager = getInstrumentManager(depth.getInstrument());
		instrumentManager.setLastDepth(depth);
		addStatistics(RECEIVE_STATS + " depth");
		depthReceived.incrementAndGet();
		algorithmNotifier.notifyObserversOnUpdateDepth(depth);
		return true;
	}

	private void checkDepth(Depth depth) {
		if (depth.isDepthFilled() && depth.getBestAsk() < depth.getBestBid()) {
			logger.warn("ask {} is lower than bid {}", depth.getBestAsk(), depth.getBestBid());
		}
	}

	@Override public boolean onTradeUpdate(Trade trade) {
		long timestamp = trade.getTimestamp();
		candleFromTickUpdater.onTradeUpdate(trade);
		if (timestamp != 0 && isBacktest) {
			timeService.setCurrentTimestamp(timestamp);
		}
		if (!isBacktest) {
			timeService.setCurrentTimestamp(new Date().getTime());
		}
		if (!checkOperationalTime()) {
			return false;
		}
		//update cache
		InstrumentManager instrumentManager = getInstrumentManager(trade.getInstrument());
		instrumentManager.setLastTrade(trade);

		addStatistics(RECEIVE_STATS + " trade");
		tradeReceived.incrementAndGet();
		algorithmNotifier.notifyObserversOnUpdateClose(trade);
		return true;
	}

	private void printBacktestResults(Map<Instrument, Table> tradesTable) {

		for (Instrument instrument : tradesTable.keySet()) {

			portfolioManager.summary(instrument);
			Table tradesInstrument = tradesTable.get(instrument);

			//			logger.info("\n\n\n");
			//			logger.info("---  {}  ---", instrument.getPrimaryKey());
			//			for (Row rowTrades : tradesInstrument) {
			//
			//				long timestamp = rowTrades.getLong("timestamp");
			//				Calendar calendar = (Calendar) timeService.getCalendar().clone();
			//				calendar.setTimeInMillis(timestamp);
			//				Date date = calendar.getTime();
			//				String verb = rowTrades.getString("verb");
			//				double price = rowTrades.getDouble("price");
			//				double quantity = rowTrades.getDouble("quantity");
			//				double position = rowTrades.getDouble("netPosition");
			//				logger.info("\t[{}] {}  {}@{}   [{}]", date, verb, quantity, price, position);
			//			}
			//			logger.info("----------");

		}

	}

	@Override public boolean onCommandUpdate(Command command) {
		//Command received from backtest
		long timestamp = command.getTimestamp();
		if (timestamp != 0 && isBacktest) {
			timeService.setCurrentTimestamp(timestamp);
		}
		if (!isBacktest) {
			timeService.setCurrentTimestamp(new Date().getTime());
		}

		logger.info("command received {}  ", command.getMessage());
		if (command.getMessage().equalsIgnoreCase(Command.ClassMessage.stop.name())) {
			String basePath = BASE_PATH_OUTPUT + File.separator + "trades_table_" + algorithmInfo;
			logger.info("Stop received => saving {} trades in {}.csv", portfolioManager.numberOfTrades, basePath);
			if (isBacktest) {
				File file = new File(basePath).getParentFile();
				file.mkdirs();//create if not exist
				Map<Instrument, Table> tradesTable = portfolioManager.getTradesTable(basePath);
				printBacktestResults(tradesTable);
			}
			if (plotStopHistorical) {
				try {
					for (InstrumentManager instrumentManager : instrumentToManager.values()) {
						Instrument instrument = instrumentManager.getInstrument();
						portfolioManager.plotHistorical(instrument);
					}
				} catch (Exception e) {
					logger.error("cant plot historical ", e);
				}
			}

			stop();

			if (isBacktest && exitOnStop) {
				System.exit(0);
			}
		}

		if (command.getMessage().equalsIgnoreCase(Command.ClassMessage.start.name())) {
			start();
		}

		return true;
	}

	/**
	 * Has to be called by the algo extending
	 *
	 * @param executionReport
	 */
	@Override public boolean onExecutionReportUpdate(ExecutionReport executionReport) {
		synchronized (EXECUTION_REPORT_LOCK) {
			updateAllActiveOrders(executionReport);

			boolean isTrade = executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled)
					|| executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.PartialFilled);
			if (isTrade) {
				addToPersist(executionReport);
				addPosition(executionReport);
				if (executionReport.getExecutionReportStatus().equals(ExecutionReportStatus.CompletellyFilled)) {
					clientOrderIdLastCompletelyFillReceived.put(executionReport.getClientOrderId(), executionReport);
				}
			}
			getQuoteManager(executionReport.getInstrument()).onExecutionReportUpdate(executionReport);
			addStatistics(RECEIVE_STATS + " executionReport." + executionReport.getExecutionReportStatus().name());
			algorithmNotifier.notifyObserversonExecutionReportUpdate(executionReport);
			return true;
		}
	}

	private void addPosition(ExecutionReport executionReport) {
		InstrumentManager instrumentManager = getInstrumentManager(executionReport.getInstrument());
		double previousPosition = instrumentManager.getPosition();
		double newPosition = previousPosition + executionReport.getLastQuantity();
		instrumentManager.setPosition(newPosition);
	}

	protected double getPosition(Instrument instrument) {
		return getInstrumentManager(instrument.getPrimaryKey()).getPosition();
	}

	private void addToPersist(ExecutionReport executionReport) {
		PnlSnapshot pnlSnapshot = portfolioManager.addTrade(executionReport);
		algorithmNotifier.notifyObserversOnUpdateTrade(pnlSnapshot);
	}

	public Date getCurrentTime() {
		return timeService.getCurrentTime();
	}

	public long getCurrentTimestamp() {
		return timeService.getCurrentTimestamp();
	}

	protected int getCurrentTimeHour() {
		return timeService.getCurrentTimeHour();
	}

	protected int getCurrentTimeDay() {
		return timeService.getCurrentTimeDay();
	}

	protected int getCurrentTimeMinute() {
		return timeService.getCurrentTimeMinute();
	}

}
