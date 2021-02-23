package com.lambda.investing.model.market_data;

import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static com.lambda.investing.model.Util.GSON_STRING;
import static com.lambda.investing.model.Util.getDatePythonUTC;
import static com.lambda.investing.model.Util.getDateUTC;

@Getter @Setter

public class Depth extends CSVable {

	public static String ALGORITHM_INFO_MM = "MarketMaker_CSV";

	private static Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	public static int MAX_DEPTH = 10;
	public static int MAX_DEPTH_CSV = 5;

	//	private transient Instrument instrument;
	private String instrument;
	private long timestamp;
	private Double[] bidsQuantities, asksQuantities, bids, asks;//TODO change to Bigdecimal

	private String[] bidsAlgorithmInfo, asksAlgorithmInfo;//just for backtesting

	private int levels;

	public double getMidPrice() {
		return (getBestBid() + getBestAsk()) / 2;
	}

	public double getSpread() {
		return Math.abs(bids[0] - asks[0]);
	}

	public boolean isDepthFilled() {
		return bids.length > 0 && asks.length > 0;
	}

	public double getBestBid() {
		return bids[0];
	}

	public double getBestAsk() {
		return asks[0];
	}

	public double getBestBidQty() {
		return bidsQuantities[0];
	}

	public double getBestAskQty() {
		return asksQuantities[0];
	}

	//TODO add more metrics VPIN ;VolumeImbalance; Microprice;

	@Override public String toString() {
		return GSON_STRING.toJson(this);
	}

	public String prettyPrint() {
		UTC_CALENDAR.setTimeInMillis(getTimestamp());
		String header = "\n----------------------------------------------\n";
		header += String.format("|\t[%s] %s\t\t\t\t|\n", UTC_CALENDAR.getTime(), getInstrument());
		header += "|\tBID\t\t\t\t\t|\t\tASK\t\t\t|\n";
		header += "----------------------------------------------\n";

		String tail = "----------------------------------------------";

		String bidRow = "|\t%.5f\t%.5f\t|\t\t\t\t\t|\n";
		String bidRowAlgos = "|\t*%.5f\t%.5f\t|\t\t\t\t\t|\n";

		String askRow = "|\t\t\t\t\t\t|\t%.5f\t%.5f\t|\n";
		String askRowAlgos = "|\t\t\t\t\t\t|\t%.5f\t%.5f*\t|\n";

		StringBuilder askSideOutput = new StringBuilder();
		askSideOutput.append(header);

		StringBuilder bidSideOutput = new StringBuilder();

		for (int level = 0; level < getLevels(); level++) {
			if (bids.length >= level + 1) {
				double bidPrice = bids[level];
				double bidQty = bidsQuantities[level];
				boolean isBidAlgo = !bidsAlgorithmInfo[level].equalsIgnoreCase(ALGORITHM_INFO_MM);
				String bidString = String.format(bidRow, bidQty, bidPrice);
				if (isBidAlgo) {
					bidString = String.format(bidRowAlgos, bidQty, bidPrice);
				}
				bidSideOutput = bidSideOutput.append(bidString);
			}

			if (asks.length >= level + 1) {
				double askPrice = asks[level];
				double askQty = asksQuantities[level];
				boolean isAskAlgo = !asksAlgorithmInfo[level].equalsIgnoreCase(ALGORITHM_INFO_MM);
				String askString = String.format(askRow, askPrice, askQty);
				if (isAskAlgo) {
					askString = String.format(askRowAlgos, askPrice, askQty);
				}
				askSideOutput = askSideOutput.append(askString);
			}

		}
		StringBuilder output = askSideOutput;
		output = output.append(bidSideOutput);
		output.append(tail);

		return output.toString();
	}

	public String toCSV(boolean withHeader) {
		if (!this.isDepthFilled()) {
			return null;
		}
		StringBuilder stringBuffer = new StringBuilder();
		if (withHeader) {
			//			,ask0,ask1,ask2,ask3,ask4,ask_quantity0,ask_quantity1,ask_quantity2,ask_quantity3,ask_quantity4,bid0,bid1,bid2,bid3,bid4,bid_quantity0,bid_quantity1,bid_quantity2,bid_quantity3,bid_quantity4
			stringBuffer
					.append(",timestamp,ask0,ask1,ask2,ask3,ask4,ask_quantity0,ask_quantity1,ask_quantity2,ask_quantity3,ask_quantity4,bid0,bid1,bid2,bid3,bid4,bid_quantity0,bid_quantity1,bid_quantity2,bid_quantity3,bid_quantity4");
			stringBuffer.append(System.lineSeparator());
		}

		//2019-11-09 08:42:24.142302
		stringBuffer.append(getDatePythonUTC(timestamp));
		stringBuffer.append(',');
		stringBuffer.append(timestamp);
		stringBuffer.append(',');

		//ask side
		for (int level = 0; level < MAX_DEPTH_CSV; level++) {
			if (level >= asks.length || asks[level] == null) {
				stringBuffer.append("");
			} else {
				stringBuffer.append(asks[level]);
			}
			stringBuffer.append(',');
		}
		for (int level = 0; level < MAX_DEPTH_CSV; level++) {
			if (level >= asksQuantities.length || asksQuantities[level] == null) {
				stringBuffer.append("");
			} else {
				stringBuffer.append(asksQuantities[level]);
			}
			stringBuffer.append(',');
		}

		//bid side
		for (int level = 0; level < MAX_DEPTH_CSV; level++) {
			if (level >= bids.length || bids[level] == null) {
				stringBuffer.append("");
			} else {
				stringBuffer.append(bids[level]);
			}
			stringBuffer.append(',');
		}
		for (int level = 0; level < MAX_DEPTH_CSV; level++) {
			if (level >= bidsQuantities.length || bidsQuantities[level] == null) {
				stringBuffer.append("");
			} else {
				stringBuffer.append(bidsQuantities[level]);
			}

			stringBuffer.append(',');
		}
		stringBuffer = stringBuffer.deleteCharAt(stringBuffer.length() - 1);//remove last comma

		return stringBuffer.toString();
	}

	@Override public Object getParquetObject() {
		return getDepthParquet();
	}

	public DepthParquet getDepthParquet() {
		return new DepthParquet(this);
	}

	public Depth() {
	}

	public Depth(DepthParquet depthParquet, Instrument instrument) {
		UTC_CALENDAR.setTimeInMillis(depthParquet.getTimestamp());
		this.instrument = instrument.getPrimaryKey();
		this.timestamp = depthParquet.getTimestamp();
		this.levels = depthParquet.getLevels();
		this.bidsQuantities = new Double[levels];
		this.asksQuantities = new Double[levels];
		this.bids = new Double[levels];
		this.asks = new Double[levels];

		if (depthParquet.getBidPrice0() != null) {
			this.bids[0] = depthParquet.getBidPrice0();
		}
		if (depthParquet.getBidPrice1() != null) {
			this.bids[1] = depthParquet.getBidPrice1();
		}
		if (depthParquet.getBidPrice2() != null) {
			this.bids[2] = depthParquet.getBidPrice2();
		}
		if (depthParquet.getBidPrice3() != null) {
			this.bids[3] = depthParquet.getBidPrice3();
		}
		if (depthParquet.getBidPrice4() != null) {
			this.bids[4] = depthParquet.getBidPrice4();
		}
		//		if (depthParquet.getBidPrice5() != null) {
		//			this.bids[5] = depthParquet.getBidPrice5();
		//		}

		if (depthParquet.getAskPrice0() != null) {
			this.asks[0] = depthParquet.getAskPrice0();
		}
		if (depthParquet.getAskPrice1() != null) {
			this.asks[1] = depthParquet.getAskPrice1();
		}
		if (depthParquet.getAskPrice2() != null) {
			this.asks[2] = depthParquet.getAskPrice2();
		}
		if (depthParquet.getAskPrice3() != null) {
			this.asks[3] = depthParquet.getAskPrice3();
		}
		if (depthParquet.getAskPrice4() != null) {
			this.asks[4] = depthParquet.getAskPrice4();
		}
		//		if (depthParquet.getAskPrice5() != null) {
		//			this.asks[5] = depthParquet.getAskPrice5();
		//		}

		if (depthParquet.getBidQuantity0() != null) {
			this.bidsQuantities[0] = depthParquet.getBidQuantity0();
		}
		if (depthParquet.getBidQuantity1() != null) {
			this.bidsQuantities[1] = depthParquet.getBidQuantity1();
		}
		if (depthParquet.getBidQuantity2() != null) {
			this.bidsQuantities[2] = depthParquet.getBidQuantity2();
		}
		if (depthParquet.getBidQuantity3() != null) {
			this.bidsQuantities[3] = depthParquet.getBidQuantity3();
		}
		if (depthParquet.getBidQuantity4() != null) {
			this.bidsQuantities[4] = depthParquet.getBidQuantity4();
		}
		//		if (depthParquet.getBidQuantity5() != null) {
		//			this.bidsQuantities[5] = depthParquet.getBidQuantity5();
		//		}

		if (depthParquet.getAskQuantity0() != null) {
			this.asksQuantities[0] = depthParquet.getAskQuantity0();
		}
		if (depthParquet.getAskQuantity1() != null) {
			this.asksQuantities[1] = depthParquet.getAskQuantity1();
		}
		if (depthParquet.getAskQuantity2() != null) {
			this.asksQuantities[2] = depthParquet.getAskQuantity2();
		}
		if (depthParquet.getAskQuantity3() != null) {
			this.asksQuantities[3] = depthParquet.getAskQuantity3();
		}
		if (depthParquet.getAskQuantity4() != null) {
			this.asksQuantities[4] = depthParquet.getAskQuantity4();
		}
		//		if (depthParquet.getAskQuantity5() != null) {
		//			this.asksQuantities[5] = depthParquet.getAskQuantity5();
		//		}
	}

	public double getMicroPrice() {
		//only on the first level
		double sumQty = getBestAskQty() + getBestBidQty();

		double out = ((getBestAsk() * getBestAskQty()) / sumQty) + ((getBestBid() * getBestBidQty()) / sumQty);
		return out;
	}

	public double getImbalance() {
		double bidVolTotal = 0.;
		double askVolTotal = 0.;

		for (int level = 0; level < getLevels(); level++) {
			try {
				bidVolTotal += bidsQuantities[level];
			} catch (IndexOutOfBoundsException e) {

			}

			try {
				askVolTotal += asksQuantities[level];
			} catch (IndexOutOfBoundsException e) {

			}
		}

		return (bidVolTotal - askVolTotal) / (bidVolTotal + askVolTotal);
	}
}

