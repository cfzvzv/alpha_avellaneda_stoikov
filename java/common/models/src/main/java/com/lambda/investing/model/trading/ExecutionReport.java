package com.lambda.investing.model.trading;

import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ExecutionReport {

	private String algorithmInfo, freeText;
	private String instrument;
	private String clientOrderId, origClientOrderId, rejectReason;
	private double price, quantity, lastQuantity, quantityFill;//todo change to bigdecimal or integer
	private ExecutionReportStatus executionReportStatus;
	private Verb verb;

	private long timestampCreation;

	/**
	 * Generates new Execution report from orderRequestPattern
	 *
	 * @param orderRequest
	 */
	public ExecutionReport(OrderRequest orderRequest) {
		this.algorithmInfo = orderRequest.getAlgorithmInfo();
		this.instrument = orderRequest.getInstrument();
		this.clientOrderId = orderRequest.getClientOrderId();
		this.origClientOrderId = orderRequest.getOrigClientOrderId();
		//		this.rejectReason=
		this.freeText = orderRequest.getFreeText();

		this.quantity = orderRequest.getQuantity();
		this.price = orderRequest.getPrice();
		this.timestampCreation = System.currentTimeMillis();//has to be updated
		this.verb = orderRequest.getVerb();
	}
}
