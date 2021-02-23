package com.lambda.investing.algorithmic_trading.avellaneda_stoikov_dqn;

import com.google.common.primitives.Doubles;
import com.lambda.investing.algorithmic_trading.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bytedeco.javacv.FrameFilter;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;

import org.deeplearning4j.core.storage.StatsStorage;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.datasets.iterator.loader.DataSetLoaderIterator;
import org.deeplearning4j.earlystopping.EarlyStoppingConfiguration;
import org.deeplearning4j.earlystopping.EarlyStoppingResult;
import org.deeplearning4j.earlystopping.saver.LocalFileModelSaver;
import org.deeplearning4j.earlystopping.scorecalc.RegressionScoreCalculator;
import org.deeplearning4j.earlystopping.termination.MaxEpochsTerminationCondition;
import org.deeplearning4j.earlystopping.termination.MaxTimeIterationTerminationCondition;
import org.deeplearning4j.earlystopping.trainer.EarlyStoppingTrainer;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;

import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.api.UIServer;
import org.deeplearning4j.ui.model.stats.StatsListener;
import org.deeplearning4j.ui.model.storage.InMemoryStatsStorage;
import org.nd4j.common.primitives.Pair;
import org.nd4j.evaluation.regression.RegressionEvaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.DataSetPreProcessor;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.Normalizer;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.datavec.api.split.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.deeplearning4j.util.ModelSerializer.restoreComputationGraphAndNormalizer;
import static org.deeplearning4j.util.ModelSerializer.restoreMultiLayerNetworkAndNormalizer;
import static org.deeplearning4j.util.ModelSerializer.writeModel;

/***
 * https://www.baeldung.com/deeplearning4j
 * https://deeplearning4j.konduit.ai/models/layers
 *
 */
public class Dl4jMemoryReplayModel implements MemoryReplayModel, Cloneable {

	//	https://deeplearning4j.konduit.ai/models/layers
	private static boolean EARLY_STOPPING = true;
	private static int SEED = 42;
	protected Logger logger = LogManager.getLogger(Dl4jMemoryReplayModel.class);
	private String modelPath;

	private double learningRate, momentumNesterov;
	private int nEpoch = 100;

	private int batchSize;
	private double l2, l1;

	private Normalizer dataNormalization;
	private MultiLayerNetwork model = null;

	private boolean isTrained = false;
	private boolean trainingStats = false;
	ScoreIterationListener scoreIterationListener = new ScoreIterationListener(10);

	public void setTrainingStats(boolean trainingStats) {
		this.trainingStats = trainingStats;
	}

	public Dl4jMemoryReplayModel(String modelPath, double learningRate, double momentumNesterov, int nEpoch,
			int batchSize, double l2, double l1) {
		this.modelPath = modelPath;
		this.learningRate = learningRate;
		this.momentumNesterov = momentumNesterov;
		this.nEpoch = nEpoch;
		this.batchSize = batchSize;
		this.l2 = l2;
		this.l1 = l1;
		loadModel();
	}


	public Dl4jMemoryReplayModel(String modelPath, double learningRate, double momentumNesterov, int nEpoch,
			int batchSize, double l2, double l1,boolean loadModel) {
		this.modelPath = modelPath;
		this.learningRate = learningRate;
		this.momentumNesterov = momentumNesterov;
		this.nEpoch = nEpoch;
		this.batchSize = batchSize;
		this.l2 = l2;
		this.l1 = l1;
		if (loadModel) {
			loadModel();
		}
	}

	public void setModelPath(String modelPath) {
		this.modelPath = modelPath;
	}


	public void loadModel() {
		File savedModelFile = new File(this.modelPath);
		if (!savedModelFile.exists()) {
			logger.info("model not found to load {}", this.modelPath);
			model = null;
			isTrained = false;
			return;
		} else {
			long start = System.currentTimeMillis();

			try {
				Pair<MultiLayerNetwork, Normalizer> pair = restoreMultiLayerNetworkAndNormalizer(savedModelFile, true);
				this.model = pair.getFirst();
				this.dataNormalization = pair.getSecond();
				this.model = MultiLayerNetwork.load(savedModelFile, true);
				isTrained = true;

				long elapsed = (System.currentTimeMillis() - start) / (1000);
				logger.info("loaded model {}", this.modelPath);
				logger.info("loaded in {} seconds , model {}", elapsed, this.modelPath);
			} catch (IOException e) {
				System.err.println(String.format("cant load model %s", this.modelPath));
				logger.error("cant load model ", e);
			}
		}

	}

	public void saveModel() {
		File savedModelFile = new File(this.modelPath);
		try {
			//			this.model.save(savedModelFile,true);
			writeModel(this.model, savedModelFile, true, (DataNormalization) this.dataNormalization);
			System.out.println(String.format("saved model %s", this.modelPath));
			logger.info(String.format("saved model %s", this.modelPath));
		} catch (IOException e) {
			System.err.println(String.format("cant save model %s", this.modelPath));
			logger.error("cant save model ", e);
		}

	}

	@Override public boolean isTrained() {
		return isTrained;
	}

	@Override public int getBatchSize() {
		return batchSize;
	}

	private MultiLayerNetwork createModel(int numInputs, int numHiddenNodes, int numOutputs, double learningRate,
			double momentumNesterov) {

		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder().seed(SEED)
				.optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
				.updater(new Nesterovs(learningRate, momentumNesterov)).l2(l2)

				//				.l2(l2).l1(l1)

				//input layer
				.list().layer(0,
						new DenseLayer.Builder().nIn(numInputs).nOut(numHiddenNodes).weightInit(WeightInit.NORMAL)
								.activation(Activation.SIGMOID).build()).
						//hidden layer 1
						layer(1,
						new DenseLayer.Builder().nIn(numHiddenNodes).nOut(numHiddenNodes).weightInit(WeightInit.NORMAL)
								.activation(Activation.SIGMOID).build()).
						//hidden layer 2
						layer(2,
						new OutputLayer.Builder(LossFunctions.LossFunction.SQUARED_LOSS).weightInit(WeightInit.NORMAL)
								.activation(Activation.SIGMOID).weightInit(WeightInit.NORMAL).nIn(numHiddenNodes)
								.nOut(numOutputs).build())

				.build();

		MultiLayerNetwork model = new MultiLayerNetwork(conf);
		model.init();

		return model;
	}

	@Override public void train(double[][] input, double[][] target) {

		INDArray x = Nd4j.create(input);
		INDArray y = Nd4j.create(target);
		final DataSet allData = new DataSet(x, y);
		final List<DataSet> list = allData.asList();
		ListDataSetIterator trainIter = new ListDataSetIterator(list);
		try {

			if (this.model == null) {
				System.out.println("Creating new empty nn");
				int numInput = input[0].length;
				int numOutput = target[0].length;
				int hiddenNodes = (numInput + numOutput);
				this.model = createModel(numInput, hiddenNodes, numOutput, learningRate, momentumNesterov);
			}

			model.addListeners(scoreIterationListener);  //Print score every 10 parameter updates
			if (this.trainingStats) {
				UIServer uiServer = UIServer.getInstance();
				StatsStorage statsStorage = new InMemoryStatsStorage();
				//			StatsStorage statsStorage = new FileStatsStorage();//in case of memory restrictions
				logger.info("starting training nn UI at localhost:9000");
				System.out.println("starting training nn UI  at localhost:9000");
				model.addListeners(new StatsListener(statsStorage));
				uiServer.attach(statsStorage);
			}



			if (EARLY_STOPPING) {
				System.out.println("starting training nn with early stop");
				DataSet allData_t = trainIter.next();
				SplitTestAndTrain testAndTrain = allData_t.splitTestAndTrain(0.75);  //Use 75% of data for training


				DataSet trainingData = testAndTrain.getTrain();
				DataSet testData = testAndTrain.getTest();

				List<DataSet> listDs = testData.asList();
				ListDataSetIterator testDataIterator = new ListDataSetIterator(listDs);

				List<DataSet> listDsTrain = trainingData.asList();
				ListDataSetIterator trainDataIterator = new ListDataSetIterator(listDsTrain);


				dataNormalization = new NormalizerStandardize();
				((NormalizerStandardize) dataNormalization).fit(trainingData);
				trainIter.setPreProcessor((DataSetPreProcessor) dataNormalization);

				EarlyStoppingConfiguration earlyStoppingConfiguration = new EarlyStoppingConfiguration.Builder()
						.iterationTerminationConditions(new MaxTimeIterationTerminationCondition(5, TimeUnit.MINUTES))
						.epochTerminationConditions(new MaxEpochsTerminationCondition(nEpoch)).scoreCalculator(
								new RegressionScoreCalculator(RegressionEvaluation.Metric.RMSE, testDataIterator))
						//						.modelSaver(new LocalFileModelSaver(this.modelPath))
						.evaluateEveryNEpochs(1).build();

				EarlyStoppingTrainer trainer = new EarlyStoppingTrainer(earlyStoppingConfiguration, this.model,
						trainDataIterator);

				EarlyStoppingResult<MultiLayerNetwork> result = trainer.fit();

				System.out.println("Termination reason: " + result.getTerminationReason());
				System.out.println("Termination details: " + result.getTerminationDetails());
				System.out.println("Total epochs: " + result.getTotalEpochs());
				System.out.println("Best epoch number: " + result.getBestModelEpoch());
				System.out.println("Score at best epoch: " + result.getBestModelScore());
				this.model = result.getBestModel();
				isTrained = true;
			} else {
				//normalize before
				System.out.println("starting training nn");
				dataNormalization = new NormalizerStandardize();
				((NormalizerStandardize) dataNormalization).fit(trainIter);
				trainIter.setPreProcessor((DataSetPreProcessor) dataNormalization);//normalize it
				this.model.fit(trainIter, this.nEpoch);
				isTrained = true;

			}

		} catch (Exception e) {
			System.err.println("error training model ");
			e.printStackTrace();
			logger.error("error training model ", e);
		}
		//persist it
		saveModel();
	}

	@Override public double[] predict(double[] input) {
		if (this.model == null) {
			//			logger.error("to predict you need to fit it first!");
			return null;
		}

		double[] output = null;
		try {
			double[][] inputArr = new double[1][input.length];
			inputArr[0] = input;
			INDArray inputND = Nd4j.create(inputArr);

			((NormalizerStandardize) dataNormalization).transform(inputND);
			INDArray outputNd = this.model.output(inputND);

			output = outputNd.toDoubleVector();
			if (Double.isNaN(Doubles.max(output))) {
				output = null;
			}
		} catch (Exception e) {
			logger.error("error predicting ", e);
		}

		return output;
	}

	@Override public MemoryReplayModel cloneIt() {
		try {
			return (Dl4jMemoryReplayModel) this.clone();
		} catch (CloneNotSupportedException e) {
			logger.error("cant clone Dl4jMemoryReplayModel ", e);
		}
		return null;

	}
}
