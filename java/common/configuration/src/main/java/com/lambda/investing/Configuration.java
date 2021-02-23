package com.lambda.investing;

import java.text.SimpleDateFormat;

public class Configuration {

	public static String getEnvOrDefault(String name, String defaultValue) {
		String output = System.getenv(name);
		if (output == null) {
			output = System.getProperty(name,defaultValue);
		}
		return output;
	}

	public static String getDataPath(){
		return getEnvOrDefault("LAMBDA_DATA_PATH", "X:\\");
	}

	//	public static String DATA_PATH = getEnvOrDefault("LAMBDA_DATA_PATH",
	//			"D:\\javif\\Coding\\cryptotradingdesk\\python\\data");
	//	public static String DATA_PATH = getEnvOrDefault("LAMBDA_DATA_PATH", "D:\\javif\\Coding\\cryptotradingdesk\\data");
	public static String DATA_PATH = getDataPath();
	public static String OUTPUT_PATH = getEnvOrDefault("LAMBDA_OUTPUT_PATH",
			"D:\\javif\\Coding\\cryptotradingdesk\\java\\output");
	public static String TEMP_PATH = getEnvOrDefault("LAMBDA_TEMP_PATH",
			"D:\\javif\\Coding\\cryptotradingdesk\\java\\temp");

	public static SimpleDateFormat FILE_CSV_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

}
