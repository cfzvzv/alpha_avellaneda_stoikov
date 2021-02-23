package com.lambda.investing.market_data_connector.parquet_file_reader;

import com.lambda.investing.Configuration;
import com.lambda.investing.model.asset.Instrument;
import com.lambda.investing.model.messaging.TypeMessage;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Getter @Setter public class ParquetFileConfiguration {

	protected Logger logger = LogManager.getLogger(ParquetFileConfiguration.class);
	private Instrument instrument;
	private int speed = -1;
	private long initialSleepSeconds = 0;
	private Date startTime, endTime;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

	private Map<Date, String> depthFilesPath;
	private Map<Date, String> tradeFilesPath;
	private List<Date> datesToLoad;

	public ParquetFileConfiguration(Instrument instrument, int speed, long initialSleepSeconds, Date startTime,
			Date endTime) {
		this.instrument = instrument;

		this.speed = speed;
		this.initialSleepSeconds = initialSleepSeconds;
		this.startTime = startTime;
		this.endTime = endTime;
		setParquetFilesPath();
	}

	public ParquetFileConfiguration(Instrument instrument, int speed, long initialSleepSeconds, String startTime,
			String endTime) throws ParseException {
		this.instrument = instrument;

		this.speed = speed;
		this.initialSleepSeconds = initialSleepSeconds;
		this.startTime = dateFormat.parse(startTime);
		this.endTime = dateFormat.parse(endTime);
		setParquetFilesPath();
	}

	public ParquetFileConfiguration(Instrument instrument, Date startTime, Date endTime) {
		this.instrument = instrument;
		this.startTime = startTime;
		this.endTime = endTime;

		setParquetFilesPath();
	}

	public static List<Date> getDaysBetweenDates(Date startdate, Date enddate) {
		TreeSet<Date> dates = new TreeSet<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startdate);
		dates.add(startdate);
		while (calendar.getTime().before(enddate)) {
			Date result = calendar.getTime();
			dates.add(result);
			calendar.add(Calendar.DATE, 1);
		}

		return new ArrayList<>(dates);
	}

	private String getPath(String type, String date, String instrumentPk) {
		return Configuration.getDataPath() + File.separator + "type=" + type + File.separator + "instrument=" + instrumentPk
				+ File.separator + "date=" + date + File.separator + "data.parquet";
	}

	private void setParquetFilesPath() {
		depthFilesPath = new HashMap<>();
		tradeFilesPath = new HashMap<>();

		//btcusd_depth_20200819.csv
		this.datesToLoad = getDaysBetweenDates(startTime, endTime);
		for (Date date : this.datesToLoad) {
			String depthFile = getPath(TypeMessage.depth.name(), dateFormat.format(date), instrument.getPrimaryKey());
			File depth = new File(depthFile);

			String tradeFile = getPath(TypeMessage.trade.name(), dateFormat.format(date), instrument.getPrimaryKey());
			File trade = new File(tradeFile);

			if (depth.exists()) {
				depthFilesPath.put(date, depthFile);

			} else {
				logger.warn("DEPTH File doesn't exist   {}", depthFile);
			}

			if (trade.exists()) {
				tradeFilesPath.put(date, tradeFile);
			} else {
				logger.warn("TRADE File doesn't exist   {}", tradeFile);
			}

		}
		logger.info("Setting File path with {} depths files and {} trades files", depthFilesPath.size(),
				tradeFilesPath.size());

	}

	public boolean isInDepthFiles(String name) {
		return depthFilesPath.containsValue(name);

	}

	public boolean isInTradeFiles(String name) {
		return tradeFilesPath.containsValue(name);

	}
}
