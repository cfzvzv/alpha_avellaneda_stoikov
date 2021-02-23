package com.lambda.investing.market_data_connector.csv_file_reader;

import com.lambda.investing.Configuration;
import com.lambda.investing.model.asset.Instrument;
import lombok.Getter;
import lombok.Setter;
import org.apache.kerby.config.Conf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Getter
@Setter
public class CSVFileConfiguration {


	protected Logger logger = LogManager.getLogger(CSVFileConfiguration.class);
	private Instrument instrument;
	private List<String> depthFilesPath;
	private List<String> tradeFilesPath;
	private int speed=-1;
	private long initialSleepSeconds=0;
	private Date startTime,endTime;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");



	public CSVFileConfiguration(Instrument instrument,  int speed, long initialSleepSeconds, Date startTime,
			Date endTime) {
		this.instrument=instrument;

		this.speed = speed;
		this.initialSleepSeconds = initialSleepSeconds;
		this.startTime = startTime;
		this.endTime = endTime;
		setCSVFilesPath();
	}


	public CSVFileConfiguration(Instrument instrument,  int speed, long initialSleepSeconds, String startTime,
			String endTime) throws ParseException {
		this.instrument=instrument;

		this.speed = speed;
		this.initialSleepSeconds = initialSleepSeconds;
		this.startTime = dateFormat.parse(startTime);
		this.endTime =  dateFormat.parse(endTime);
		setCSVFilesPath();
	}

	public List<Date> getDays() {
		return getDaysBetweenDates(this.startTime, this.endTime);
	}

	public CSVFileConfiguration(Instrument instrument,Date startTime,
			Date endTime) {
		this.instrument=instrument;
		this.startTime=startTime;
		this.endTime=endTime;

		setCSVFilesPath();
	}

	public static List<Date> getDaysBetweenDates(Date startdate, Date enddate)
	{
		List<Date> dates = new ArrayList<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startdate);
		dates.add(startdate);
		while (calendar.getTime().before(enddate))
		{
			Date result = calendar.getTime();
			dates.add(result);
			calendar.add(Calendar.DATE, 1);
		}

		return new ArrayList<>(new HashSet<>(dates));
	}

	private void setCSVFilesPath(){
		depthFilesPath=new ArrayList<>();
		tradeFilesPath = new ArrayList<>();
		//btcusd_depth_20200819.csv
		List<Date> listOfDates = getDaysBetweenDates(startTime, endTime);
		String pathPrefix = Configuration.getDataPath() + File.separator+this.instrument.getPrimaryKey();

		for (Date date :listOfDates){
			String depthFile =pathPrefix+"_depth_"+dateFormat.format(date)+".csv";
			File depth = new File(depthFile);


			String tradeFile =pathPrefix+"_trade_"+dateFormat.format(date)+".csv";
			File trade = new File(tradeFile);

			if(depth.exists()) {
				depthFilesPath.add(depthFile);
			}else{
				logger.warn("DEPTH File doesn't exist   {}",depthFile);
			}


			if(trade.exists()) {
				tradeFilesPath.add(tradeFile);
			}else{
				logger.warn("TRADE File doesn't exist   {}",tradeFile);
			}

		}
		logger.info("Setting File path with {} depths files and {} trades files",depthFilesPath.size(),tradeFilesPath.size());

	}

	public boolean isInDepthFiles(String name) {
		String pathComplete = Configuration.getDataPath() + File.separator + name;
		return depthFilesPath.contains(pathComplete);

	}

	public boolean isInTradeFiles(String name) {
		String pathComplete = Configuration.getDataPath() + File.separator + name;
		return tradeFilesPath.contains(pathComplete);

	}
}
