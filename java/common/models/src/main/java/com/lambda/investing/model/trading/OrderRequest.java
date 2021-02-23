package com.lambda.investing.model.trading;

import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter public class OrderRequest {

	private String instrument;
	private OrderRequestAction orderRequestAction;
	private double price, quantity;//todo change to bigdecimal or integer
	private Verb verb;
	private OrderType orderType;
	private MarketOrderType marketOrderType;
	private String clientOrderId, origClientOrderId;

	private long timestampCreation;

	private String algorithmInfo;
	private String freeText;

	@Override public String toString() {
		if (orderRequestAction.equals(OrderRequestAction.Send)) {
			String output = String
					.format("Send %s %s ->%s [%s]%.4f@%.3f", instrument, algorithmInfo, verb, clientOrderId, quantity,
							price);
			return output;
		} else if (orderRequestAction.equals(OrderRequestAction.Modify)) {
			String output = String
					.format("Modification %s %s %s->%s [%s]%.4f@%.3f", instrument, algorithmInfo, origClientOrderId,
							verb, clientOrderId, quantity, price);
			return output;
		} else if (orderRequestAction.equals(OrderRequestAction.Cancel)) {
			String output = String
					.format("Cancel %s %s %s->%s cancel", instrument, algorithmInfo, origClientOrderId, clientOrderId);
			return output;
		} else {
			return "uknown action " + orderRequestAction + " " + clientOrderId + " " + super.toString();
		}

	}
}
