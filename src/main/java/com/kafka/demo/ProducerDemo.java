package com.kafka.demo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("zk.connect", "weekend01:2181,weekend02:2181,weekend03:2181");
		props.put("metadata.broker.list","weekend01:9092,weekend02:9092,weekend03:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		for (int i = 1; i <= 100; i++) {
			Thread.sleep(500);
			producer.send(new KeyedMessage<String, String>("wordcount",
					"i said i love you baby for" + i + "times,will you have a nice day with me tomorrow"));
		}

	}
}