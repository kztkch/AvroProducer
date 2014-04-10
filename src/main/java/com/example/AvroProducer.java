package com.example;

import java.io.File;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

public class AvroProducer { 
	private static Producer<Integer, String> producer; 
	private final Properties props = new Properties(); 
	public AvroProducer() { 
		props.put("metadata.broker.list", "kafka-01:9092"); 
		props.put("serializer.class", "kafka.serializer.StringEncoder"); 
		props.put("request.required.acks", "1"); 
		producer = new Producer<Integer, String>(new ProducerConfig(props)); 
	} 
	
	public static void main(String[] args) throws Exception { 
		Schema schema = new Parser().parse(new File("./src/main/avro/DummyLog.avsc")); 

		//Using this schema, let's create some users. 
		AvroProducer sp = new AvroProducer(); 
		String topic = args[0];

		GenericRecord user1 = new GenericData.Record(schema); 
		user1.put("id", 100); 
		user1.put("logTime", 500); 

		KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String> (topic, user1.toString()); 
		int i=0; 
		while (i<100) { 
			producer.send(data); i++; 
		} 
		producer.close(); 
	} 
} 
