package com.lambda.investing.binance;

import com.binance.api.client.BinanceApiAsyncRestClient;
import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.BinanceApiWebSocketClient;
import lombok.Getter;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter public class BinanceBrokerConnector {

	public static NumberFormat NUMBER_FORMAT = NumberFormat
			.getInstance(Locale.US);//US has dot instead of commas in decimals

	private static Map<String, BinanceBrokerConnector> instances = new ConcurrentHashMap<>();

	public static BinanceBrokerConnector getInstance(String apiKey, String secretKey) {
		String key = apiKey + secretKey;
		BinanceBrokerConnector output = instances.getOrDefault(key, new BinanceBrokerConnector(apiKey, secretKey));
		instances.put(key, output);
		return output;
	}

	private BinanceApiWebSocketClient webSocketClient;
	private BinanceApiAsyncRestClient asyncRestClient;
	private BinanceApiRestClient restClient;

	private BinanceBrokerConnector(String apiKey, String secretKey) {
		BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance(apiKey, secretKey);
		this.restClient = factory.newRestClient();
		this.webSocketClient = factory.newWebSocketClient();
		this.asyncRestClient = factory.newAsyncRestClient();
	}

}
