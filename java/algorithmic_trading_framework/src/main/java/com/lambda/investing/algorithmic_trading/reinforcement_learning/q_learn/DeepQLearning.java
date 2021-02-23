package com.lambda.investing.algorithmic_trading.reinforcement_learning.q_learn;

import com.google.common.primitives.Doubles;
import com.lambda.investing.algorithmic_trading.LogLevels;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn.Dl4jMemoryReplayModel;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn.MemoryReplayModel;
import com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn.OnnxMemoryReplayModel;
import com.lambda.investing.algorithmic_trading.reinforcement_learning.action.AbstractAction;
import com.lambda.investing.algorithmic_trading.reinforcement_learning.action.AvellanedaAction;
import com.lambda.investing.algorithmic_trading.reinforcement_learning.q_learn.exploration_policy.EpsilonGreedyExploration;
import com.lambda.investing.algorithmic_trading.reinforcement_learning.state.AbstractState;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

import static com.lambda.investing.algorithmic_trading.Algorithm.LOG_LEVEL;

@Getter @Setter public class DeepQLearning extends QLearning {
	public static boolean TRAIN_FROM_FILE_MODEL= false;
	private static Comparator<double[]> ARRAY_COMPARATOR = Doubles.lexicographicalComparator();
	protected static Logger logger = LogManager.getLogger(DeepQLearning.class);
	private static double DEFAULT_PREDICTION_ACTION_SCORE = -1;

	private static int MAX_MEMORY_SIZE = (int) 1E6;
	private double epsilon;
	private Random r = new Random();
	private AbstractState state;
	private AbstractAction action;
	private int memoryReplayIndex, maxMemorySize, memoryReplaySize;
	private StateRow[] stateRowSet;
	private MemoryReplayModel predictModel, targetModel;

	private double[] defaultActionPredictScore;

	/**
	 * Initializes a new instance of the QLearning class.
	 *
	 * @param state             class states.
	 * @param action            class actions.
	 * @param explorationPolicy not used
	 */
	public DeepQLearning(AbstractState state, AbstractAction action, IExplorationPolicy explorationPolicy,
			int maxMemorySize, MemoryReplayModel predictModel, MemoryReplayModel targetModel) throws Exception {
		super(Integer.MAX_VALUE, action.getNumberActions(), explorationPolicy);
		if (maxMemorySize <= 0) {
			maxMemorySize = MAX_MEMORY_SIZE;
		}
		this.maxMemorySize = maxMemorySize;
		this.state = state;
		this.action = action;
		this.stateRowSet = new StateRow[this.maxMemorySize];
		this.predictModel = predictModel;
		this.targetModel = targetModel;
		if (!(explorationPolicy instanceof EpsilonGreedyExploration)) {
			throw new Exception("DeepQLearning only available with epsilon greedy!");
		}

		// create Q-array
		int numberOfColumns =
				getStateColumns() + action.getNumberActions() + getStateColumns();//state action next_state
		memoryReplay = new double[this.maxMemorySize][numberOfColumns];
		epsilon = ((EpsilonGreedyExploration) explorationPolicy).getEpsilon();
	}

	public void setSeed(long seed){
		r.setSeed(seed);
	}

	public static boolean trainOnData(String memoryPath, int actionColumns, int stateColumns, String outputModelPath,
			double learningRate, double momentumNesterov, int nEpoch, int batchSize, double l2, double l1,
			int trainingStats) throws IOException {
		File file = new File(memoryPath);
		if (!file.exists()) {
			System.err.println(memoryPath + " not exist to train");
			return false;
		}
		double[][] memoryData = loadCSV(memoryPath, stateColumns);
		//check load dimension
		int columnsRead = memoryData[0].length;
		assert columnsRead == (stateColumns * 2) + actionColumns;

		if (batchSize <= 0 && memoryData != null) {
			batchSize = memoryData.length;
		}

		Dl4jMemoryReplayModel memoryReplayModel = new Dl4jMemoryReplayModel(outputModelPath, learningRate,
				momentumNesterov, nEpoch, batchSize, l2, l1,TRAIN_FROM_FILE_MODEL);

		if (trainingStats != 0) {
			memoryReplayModel.setTrainingStats(true);
		}

		double[][] x = getColumnsArray(memoryData, 0, stateColumns);
		double[][] y = getColumnsArray(memoryData, stateColumns, stateColumns + actionColumns);
		logger.info("starting training model with {} epoch on {} batch", nEpoch, batchSize);
		long start = System.currentTimeMillis();
		memoryReplayModel.train(x, y);
		long elapsed = (System.currentTimeMillis() - start) / (1000 * 60);
		logger.info("trained finished on {} minutes ,saving model {}", elapsed, outputModelPath);
		memoryReplayModel.saveModel();
		return true;

	}

	public void setPredictModel(MemoryReplayModel predictModel) {
		this.predictModel = predictModel;
	}

	public void setTargetModel(MemoryReplayModel targetModel) {
		this.targetModel = targetModel;
	}

	private int getStateColumns() {
		return state.getNumberOfColumns();
	}

	public void saveMemory(String filepath) throws IOException {
		if (memoryReplayIndex <= 0) {
			logger.warn("no data in DeepQlearning memoryReplay to save!");
			return;
		}
		File file = new File(filepath);
		file.getParentFile().mkdirs();
		StringBuilder outputString = new StringBuilder();
		for (int row = 0; row < memoryReplayIndex; row++) {
			for (int column = 0; column < memoryReplay[row].length; column++) {
				outputString.append(memoryReplay[row][column]);
				outputString.append(CSV_SEPARATOR);
			}
			outputString.append(System.lineSeparator());
		}

		outputString = outputString.delete(outputString.lastIndexOf(System.lineSeparator()),
				outputString.length());//remove last line separator

		String content = outputString.toString();
		BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
		try {
			writer.write(content);
			logger.info("saved memory replay size of  {}/{} rows and {} states-actions-next-states to {}",
					memoryReplaySize, maxMemorySize, memoryReplay[0].length, filepath);
			System.out.println("saved memory " + memoryReplaySize + " rows into " + filepath);
		} catch (Exception e) {
			logger.error("error saving memory replay to file {} ", filepath, e);
		} finally {
			writer.close();
		}
	}

	private static double[][] loadCSV(String filepath, int columnsStates) throws IOException {
		//only used on trainOnData
		File file = new File(filepath);
		if (!file.exists()) {
			logger.warn("memory not found {}-> start empty", filepath);
			return null;
		}

		BufferedReader csvReader = new BufferedReader(new FileReader(filepath));
		// we don't know the amount of data ahead of time so we use lists

		Map<Integer, List<Double>> colMap = new HashMap<>();
		String row;
		int rowsTotal = 0;

		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(CSV_SEPARATOR);
			double[] stateRow = new double[columnsStates];
			for (int column = 0; column < data.length; column++) {
				List<Double> columnList = colMap.getOrDefault(column, new ArrayList<>());
				double value = Double.parseDouble(data[column]);
				columnList.add(value);
				colMap.put(column, columnList);

				if (column < stateRow.length) {
					stateRow[column] = value;
				}

			}
			rowsTotal++;
		}
		csvReader.close();
		int columnsTotal = colMap.size();

		//transform colMap into array
		double[][] loadedQvalues = new double[rowsTotal][columnsTotal];//states rows , actions columns
		int rowsFilled = 0;
		for (int column : colMap.keySet()) {
			List<Double> rows = colMap.get(column);
			int rowIter = 0;
			for (double rowVal : rows) {
				loadedQvalues[rowIter][column] = rowVal;
				rowsFilled = rowIter;
				rowIter++;

			}
		}

		//		loadedQvalues=ArrayUtils.subarray(loadedQvalues, 0, rowsTotal);

		System.out.println(
				String.format("loaded a memory replay of %d/%d rows-states   and %d states-actions-next-states",
						rowsFilled, loadedQvalues.length, loadedQvalues[0].length));

		return loadedQvalues;

	}

	public void loadMemory(String filepath) throws IOException {
		File file = new File(filepath);
		if (!file.exists()) {
			logger.warn("memory not found {}-> start empty", filepath);
			return;
		}
		BufferedReader csvReader = new BufferedReader(new FileReader(filepath));
		// we don't know the amount of data ahead of time so we use lists

		Map<Integer, List<Double>> colMap = new HashMap<>();
		String row;
		int rowsTotal = 0;

		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(CSV_SEPARATOR);
			double[] stateRow = new double[state.getNumberOfColumns()];
			for (int column = 0; column < data.length; column++) {
				List<Double> columnList = colMap.getOrDefault(column, new ArrayList<>());
				double value = Double.parseDouble(data[column]);
				columnList.add(value);
				colMap.put(column, columnList);

				if (column < stateRow.length) {
					stateRow[column] = value;
				}

			}
			try {
				stateRowSet[rowsTotal] = new StateRow(stateRow);
			} catch (IndexOutOfBoundsException e) {
				logger.warn("IndexOutOfBoundsException loading memory -> loading first {} rows -> set as index ,size",
						rowsTotal);
				memoryReplayIndex = -1;
				break;
			}
			memoryReplaySize = rowsTotal;//starts at 0
			memoryReplayIndex = rowsTotal;
			rowsTotal++;
		}
		csvReader.close();
		int columnsTotal = colMap.size();

		int actionColumns = columnsTotal - 2 * state.getNumberOfColumns();
		if (actionColumns != action.getNumberActions()) {
			System.err.println("cant load " + filepath + " columns are not equal!-> starting from empty memory");
			logger.error("cant load " + filepath + " columns are not equal!-> starting from empty memory");
			return;
		}

		//transform colMap into array

		double[][] loadedQvalues = new double[this.maxMemorySize][columnsTotal];//states rows , actions columns
		for (int column : colMap.keySet()) {
			List<Double> rows = colMap.get(column);
			int rowIter = 0;
			for (double rowVal : rows) {
				try {
					loadedQvalues[rowIter][column] = rowVal;
					rowIter++;
				} catch (IndexOutOfBoundsException e) {
					this.memoryReplaySize = rowIter;//starts at 1
					break;
				}
			}
		}

		this.memoryReplay = loadedQvalues;
		System.out.println(String.format(
				"loaded a memory replay of %d/%d rows-states and %d states-actions-next-states on a %d maxMemorySize and index start on %d",
				rowsTotal, this.memoryReplay.length, this.memoryReplay[0].length, this.maxMemorySize,
				this.memoryReplayIndex));

		logger.info(String.format(
				"loaded a memory replay of %d/%d rows-states and %d states-actions-next-states on a %d maxMemorySize and index start on %d from %s",
				rowsTotal, this.memoryReplay.length, this.memoryReplay[0].length, this.maxMemorySize,
				this.memoryReplayIndex,
				filepath));

		iterateIndex();

	}

	private void iterateIndex() {
		if (memoryReplayIndex > maxMemorySize - 1) {
			//complete memory => restart from beginning
			memoryReplayIndex = 0;
		} else {
			memoryReplayIndex++;
		}

		this.memoryReplaySize++;//0-49
		this.memoryReplaySize = Math.min(memoryReplaySize, maxMemorySize - 1);

	}

	/**
	 * PREDICT Network
	 *
	 * @param state
	 * @return
	 */
	public double[] getPredict(double[] state) {
		double[] output = this.predictModel.predict(state);
		return output;
		//		if (output==null){
		//			if (defaultActionPredictScore==null){
		//				defaultActionPredictScore = new double[getActions()];
		//				Arrays.fill(defaultActionPredictScore, DEFAULT_PREDICTION_ACTION_SCORE);
		//			}
		//			output = defaultActionPredictScore.clone();
		//			return output;
		//		}else{
		//			return output;
		//		}
	}

	/**
	 * TARGET network
	 *
	 * @param state
	 * @return
	 */
	public double getPredictNextStateBestReward(double[] state) {

		try {
			double[] actionArr = this.targetModel.predict(state);
			if (actionArr == null) {
				return Double.NaN;
			}
			return Doubles.max(actionArr);
		} catch (Exception e) {
			return Double.NaN;
		}
	}

	public int GetAction(AbstractState lastState) {
		double[] currentState = lastState.getCurrentStateRounded();
		double[] actionScoreEstimation = getPredict(currentState);

		int greedyAction = -1;
		int index = 0;
		if (actionScoreEstimation != null) {
			double bestScore = Doubles.max(actionScoreEstimation);
			for (double score : actionScoreEstimation) {
				if (score > bestScore) {
					greedyAction = index;
				}
				index++;
			}
		}
		// try to do exploration
		if (greedyAction == -1 || r.nextDouble() < epsilon) {
			int randomAction = r.nextInt(getActions() - 1);

			//why?
			//			if (randomAction >= greedyAction)
			//				randomAction++;

			return randomAction;
		}

		return greedyAction;

	}

	public int GetAction(int state) {
		System.err.println("GetAction:int not used in dqn");
		return -1;
	}

	public void updateState(int previousState, int action, double reward, int nextState) {
		System.err.println("updateState:int not used in dqn");
	}

	private int getInputSizeLimit() {
		if (this.memoryReplaySize > this.maxMemorySize - 1) {
			return this.maxMemorySize;
		} else {
			return this.memoryReplaySize;
		}

	}

	private StateRow[] getSubsetAllStates() {
		int limit = getInputSizeLimit();
		if (limit < this.maxMemorySize) {
			limit = limit - 1;//not all updated yet and pointing to the next row-> discard it
		}

		while (true) {
			StateRow[] subset = ArrayUtils
					.subarray(stateRowSet, 0, limit);//include the end limit because is not updated yet!
			if (subset == null || subset.length == 0) {
				return null;
			}

			try {
				Arrays.sort(subset);
				return subset;
			} catch (NullPointerException e) {
				logger.error(
						"NullPointerException Arrays.sort strange with limit:{}  memoryReplaySize:{}  maxMemorySize:{} -> less limit",
						limit, memoryReplaySize, maxMemorySize, e);
				limit--;
			}

		}
	}

	private int stateExistRow(double[] previousStateArr) {
		if (this.memoryReplaySize == 0) {
			return -1;
		}

		StateRow[] subset = getSubsetAllStates();
		if (subset == null || subset.length == 0) {
			return -1;
		}

		int indexOfState = Arrays.binarySearch(subset, new StateRow(previousStateArr));
		if (indexOfState > 0 && !ArrayUtils.isEquals(previousStateArr, subset[indexOfState].inputArray)) {
			//different state checking ,prevent mix them-> not found
			return -1;
		}

		if (indexOfState > 0 && LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
			logger.info("state found {} -> {}", indexOfState, ArrayUtils.toString(previousStateArr));
		}
		if (indexOfState <= 0 && LOG_LEVEL > LogLevels.SOME_ITERATION_LOG.ordinal()) {
			logger.info("state not found  -> {}", ArrayUtils.toString(previousStateArr));
		}
		return indexOfState;

	}

	public void updateState(double[] previousStateArr, int action, double reward, AbstractState nextState) {
		boolean isNewRow = true;
		int indexOfState = stateExistRow(previousStateArr);
		if (indexOfState > -1) {
			isNewRow = false;
		}

		double[] actionArr = new double[getActions()];
		if (indexOfState > -1) {
			double[] arrayRow = memoryReplay[indexOfState];
			double[] stateArr = ArrayUtils.subarray(memoryReplay[indexOfState], 0, state.getNumberOfColumns());
			actionArr = ArrayUtils.subarray(memoryReplay[indexOfState], state.getNumberOfColumns(),
					state.getNumberOfColumns() + this.action.getNumberActions());
		}
		double maxNextExpectedReward = getPredictNextStateBestReward(
				nextState.getCurrentStateRounded());//from target network

		if (Double.isNaN(maxNextExpectedReward)) {
			//			logger.error("something is wrong on prediciton model => expected next reward nan");
			//			System.err.println("something is wrong on prediction model => expected next reward nan");
			maxNextExpectedReward = DEFAULT_PREDICTION_ACTION_SCORE;
		}

		// previous state's action estimations
		try {
			// update expexted summary reward of the previous state
			actionArr[action] *= (1.0 - learningRate);
			actionArr[action] += (learningRate * (reward + discountFactor * maxNextExpectedReward));
		} catch (IndexOutOfBoundsException e) {
			System.err.println(
					"Trying to save action index " + action + " in an actions array of len " + actionArr.length);
			throw e;
		}

		if (isNewRow) {
			double[] nextStateArr = nextState.getCurrentStateRounded();
			double[] newRow = ArrayUtils.addAll(previousStateArr, actionArr);
			newRow = ArrayUtils.addAll(newRow, nextStateArr);
			try {
				this.memoryReplay[this.memoryReplayIndex] = newRow;
				this.stateRowSet[this.memoryReplayIndex] = new StateRow(previousStateArr);
			} catch (IndexOutOfBoundsException e) {
				logger.error(
						"out of bounds error updating memoryReplay with memoryReplayIndex {} and maxMemorySize {} -> save to zero",
						memoryReplayIndex, maxMemorySize);
				this.memoryReplayIndex = 0;
				this.memoryReplay[this.memoryReplayIndex] = newRow;
			}
			iterateIndex();

		}

	}

	public static double[][] getColumnsArray(double[][] input, int firstColumn, int lastColumn) {
		double[][] output = new double[input.length][lastColumn - firstColumn];
		for (int row = 0; row < input.length; row++) {
			for (int column = firstColumn; column < lastColumn; column++) {
				// index starts from 0
				output[row][column - firstColumn] = input[row][column];
			}

		}
		return output;
	}

	public double[][] getInputTrain() {
		double[][] validArr = ArrayUtils.subarray(memoryReplay, 0, getInputSizeLimit());
		logger.info("training input array of {} rows and {} columns", getInputSizeLimit(), getStateColumns());
		return getColumnsArray(validArr, 0, getStateColumns());
	}

	public double[][] getTargetTrain() {
		double[][] validArr = ArrayUtils.subarray(memoryReplay, 0, getInputSizeLimit());
		logger.info("training target array of {} rows and {} columns", validArr.length, action.getNumberActions());
		return getColumnsArray(validArr, getStateColumns(), getStateColumns() + action.getNumberActions());
	}

	/***
	 * Class used to have index of the states saved
	 */
	private class StateRow implements Comparable<StateRow> {

		private double[] inputArray;

		public StateRow(double[] inputArray) {
			this.inputArray = inputArray;
		}

		@Override public boolean equals(Object o) {
			if (this == o)
				return true;
			if (!(o instanceof StateRow))
				return false;
			StateRow stateRow = (StateRow) o;
			return Arrays.equals(inputArray, stateRow.inputArray);
		}

		@Override public int hashCode() {
			return Arrays.hashCode(inputArray);
		}

		@Override public int compareTo(StateRow o) {
			if (o.equals(this.inputArray)) {
				return 0;
			}
			return ARRAY_COMPARATOR.compare(inputArray, o.inputArray);
		}

	}

}
