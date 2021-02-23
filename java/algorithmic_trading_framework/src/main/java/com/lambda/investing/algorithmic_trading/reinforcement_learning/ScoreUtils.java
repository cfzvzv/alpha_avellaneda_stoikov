package com.lambda.investing.algorithmic_trading.reinforcement_learning;

import com.lambda.investing.algorithmic_trading.Algorithm;
import com.lambda.investing.algorithmic_trading.PnlSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScoreUtils {

	static Logger logger = LogManager.getLogger(Algorithm.class);

	public static double getReward(ScoreEnum scoreEnum, PnlSnapshot pnlSnapshot) {

		if (scoreEnum.equals(ScoreEnum.realized_pnl)) {
			return pnlSnapshot.getRealizedPnl();
		} else if (scoreEnum.equals(ScoreEnum.total_pnl)) {
			return pnlSnapshot.getTotalPnl();
		} else if (scoreEnum.equals(ScoreEnum.asymmetric_dampened_pnl)) {
			double speculative = Math.min(pnlSnapshot.unrealizedPnl, 0.);
			return (pnlSnapshot.realizedPnl + speculative);
		} else if (scoreEnum.equals(ScoreEnum.unrealized_pnl)) {
			return (pnlSnapshot.unrealizedPnl);
		} else {
			logger.error("{} not found to calculate score", scoreEnum);
			return -1;
		}
	}

}
