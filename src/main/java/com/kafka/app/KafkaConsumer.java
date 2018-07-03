package com.kafka.app;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaConsumer {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Sampleapp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("test");

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
				
		  KafkaUtils.createDirectStream(jssc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

		
		JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() 
		{
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception 
            {
                return kafkaRecord.value();
            }
		});
		
		
		
		 JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	            @Override
	            public Iterator<String> call(String line) throws Exception {
	                return Arrays.asList(line.split(" ")).iterator();
	            }
	        });

		 words.print();
		 
		 
		 JavaPairDStream<String, Integer> lenghtWords = words.mapToPair(new PairFunction<String,String,Integer>(){
			 @Override
			 public Tuple2<String,Integer> call(String word){
				 return new Tuple2<>(word,word.length());
			 }
			 
		 });
		 
	        // Take every word and return Tuple with (word,1)
	        JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
	            @Override
	            public Tuple2<String, Integer> call(String word) throws Exception {
	                return new Tuple2<>(word,1);
	            }
	        });
	        
	        
	        
	        
	       //wordMap.print();
	        
	        // Count occurance of each word
	        JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
	             public Integer call(Integer first, Integer second) throws Exception {
	                 return first+second;
	             }
	         });

	        //Print the word count
	        wordCount.print();
	        
	        

	        jssc.start();
	try {
		jssc.awaitTermination();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	}
}
