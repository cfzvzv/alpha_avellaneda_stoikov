package com.lambda.investing.backtest;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter public class TrainInputConfiguration {

	//	int numberActions,int stateColumns,String outputModelPath,double learningRate,double momentumNesterov,int nEpoch,int batchSize,double l2,double l1
	private String memoryPath, outputModelPath;
	private int actionColumns, stateColumns, nEpoch, batchSize;

	private double l2 = 0.0001;
	private double l1 = 0.;
	private double learningRate = 0.25;
	private double momentumNesterov = 0.5;
	private int trainingStats = 0;

	public TrainInputConfiguration() {
	}

}
