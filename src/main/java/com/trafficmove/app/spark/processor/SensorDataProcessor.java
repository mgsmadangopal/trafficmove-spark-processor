package com.trafficmove.app.spark.processor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.spark.api.java.Optional;

import com.trafficmove.app.spark.vo.POIData;
import com.trafficmove.avro.SensorData;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import scala.Tuple2;
import scala.Tuple3;

/**
 * This class consumes Kafka avro messages and creates stream for processing the sensor data.
 */
public class SensorDataProcessor {
	
	 private static final Logger logger = Logger.getLogger(SensorDataProcessor.class);
	
	 public static void main(String[] args) throws Exception {
		 //read Spark and Cassandra properties and create SparkConf
		 //Properties prop = PropertyFileReader.readPropertyFile();
		 
		//read config file		
			
		Config appConfig = ConfigFactory.defaultApplication().resolve();					
		 
		SparkConf conf = new SparkConf()
				 .setAppName(appConfig.getString("app.name"))
				 .setMaster("local[2]")
				 .set("spark.cassandra.connection.host", appConfig.getString("cassandra.host"))
				 .set("spark.cassandra.connection.port", appConfig.getString("cassandra.port"))
				 .set("spark.cassandra.connection.keep_alive_ms", appConfig.getString("cassandra.keep_alive"));
		
		 //batch interval of 5 seconds for incoming stream		 
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));	
		
		 //add check point directory
		 jssc.checkpoint(appConfig.getString("checkpoint.dir"));
		 
		 //read and set Kafka properties
		 Map<String, Object> kafkaParams = new HashMap<String, Object>();				 
		 kafkaParams.put("bootstrap.servers", appConfig.getString("bootstrapservers"));			
		 kafkaParams.put("group.id", "clicks_avro_stream_consumers");
		 kafkaParams.put("auto.offset.reset", "earliest");		 
		 kafkaParams.put("enable.auto.commit", true);
		 kafkaParams.put("schema.registry.url", appConfig.getString("schemaregistryurl"));
		 kafkaParams.put("specific.avro.reader", true);		 
		 kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		 kafkaParams.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		 
		 System.out.println("Starting Stream Processor...");
		 
		 String topic = appConfig.getString("topic");
		 
		 Set<String> topicsSet = new HashSet<String>();
		 topicsSet.add(topic);
		 
		 //create direct kafka stream		
		 
		 JavaInputDStream<ConsumerRecord<String, SensorData>> directKafkaStream = KafkaUtils.createDirectStream(jssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, SensorData>Subscribe(topicsSet, kafkaParams));
		 
		 logger.info("Starting Stream Processing");
		 
		 //We need non filtered stream for poi traffic data calculation
		 JavaDStream<SensorData> nonFilteredSensorDataStream = directKafkaStream.map(tuple -> tuple.value());
		 
		 //We need filtered stream for total and traffic data calculation
		 //JavaPairDStream<String,SensorData> sensorDataPairStream = nonFilteredSensorDataStream.mapToPair(sensor -> new Tuple2<String,SensorData>(sensor.getVehicleId(),sensor)).reduceByKey((a, b) -> a );
		 JavaPairDStream<String,SensorData> sensorDataPairStream = nonFilteredSensorDataStream.mapToPair(sensor -> new Tuple2<String,SensorData>(sensor.getVehicleId(),sensor)).reduceByKey((a, b) -> a );
		
		 // Check vehicle Id is already processed
		 JavaMapWithStateDStream<String, SensorData, Boolean, Tuple2<SensorData,Boolean>> sensorDStreamWithStatePairs = sensorDataPairStream				 
							.mapWithState(StateSpec.function(processedVehicleFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

		 // Filter processed vehicle ids and keep un-processed
		 JavaDStream<Tuple2<SensorData,Boolean>> filteredSensorDStreams = sensorDStreamWithStatePairs.map(tuple2 -> tuple2)
							.filter(tuple -> tuple._2.equals(Boolean.FALSE));

		 // Get stream of SensorData
		 JavaDStream<SensorData> filteredSensorDataStream = filteredSensorDStreams.map(tuple -> tuple._1);
		 
		 //cache stream as it is used in total and window based computation
		 filteredSensorDataStream.cache();
		 	 
		 //process data
		 SensorTrafficDataProcessor iotTrafficProcessor = new SensorTrafficDataProcessor();
		 iotTrafficProcessor.processTotalTrafficData(filteredSensorDataStream);
		 iotTrafficProcessor.processWindowTrafficData(filteredSensorDataStream);

		 //poi data
		 POIData poiData = new POIData();
		 poiData.setLatitude(34.06454);
		 poiData.setLongitude(-96.594215);
		 poiData.setRadius(30);//30 km
		 
		 //broadcast variables. We will monitor vehicles on Route 37 which are of type Truck
		 Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues = jssc.sparkContext().broadcast(new Tuple3<>(poiData,"Route-1","Bus"));
		 //call method  to process stream
		 iotTrafficProcessor.processPOIData(nonFilteredSensorDataStream,broadcastPOIValues);
		 
		 //start context
		 jssc.start();    
		 jssc.awaitTermination();  
  }
	 //Funtion to check processed vehicles.
	private static final Function3<String, Optional<SensorData>, State<Boolean>, Tuple2<SensorData,Boolean>> processedVehicleFunc = (String, sensor, state) -> {
			Tuple2<SensorData,Boolean> vehicle = new Tuple2<>(sensor.get(),false);
			if(state.exists()){
				vehicle = new Tuple2<>(sensor.get(),true);
			}else{
				state.update(Boolean.TRUE);
			}			
			return vehicle;
		};
          
}
