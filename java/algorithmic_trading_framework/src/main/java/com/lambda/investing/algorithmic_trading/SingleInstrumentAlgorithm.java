package com.lambda.investing.algorithmic_trading;

import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter @Setter public abstract class SingleInstrumentAlgorithm extends Algorithm {

	protected Instrument instrument;

	public SingleInstrumentAlgorithm(AlgorithmConnectorConfiguration algorithmConnectorConfiguration,
			String algorithmInfo, Map<String, Object> parameters) {
		super(algorithmConnectorConfiguration, algorithmInfo, parameters);
	}

	public SingleInstrumentAlgorithm(String algorithmInfo, Map<String, Object> parameters) {
		super(algorithmInfo, parameters);
	}

	public InstrumentManager getInstrumentManager() {
		return getInstrumentManager(instrument.getPrimaryKey());
	}

}
