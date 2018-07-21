package com.trafficmove.app.spark.processor;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.trafficmove.app.spark.entity.POITrafficData;
import com.trafficmove.app.spark.entity.TotalTrafficData;
import com.trafficmove.app.spark.entity.WindowTrafficData;
import com.trafficmove.app.spark.util.GeoDistanceCalculator;
import com.trafficmove.app.spark.vo.AggregateKey;
import com.trafficmove.app.spark.vo.POIData;
import com.trafficmove.avro.SensorData;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import scala.Tuple3;


public class SensorTrafficDataProcessor {
	private static final Logger logger = Logger.getLogger(SensorTrafficDataProcessor.class);

	/**
	 * Method to get total traffic counts of different type of vehicles for each route.
	 * 
	 * @param filteredSensorDataStream sensor data stream
	 */
	public void processTotalTrafficData(JavaDStream<SensorData> filteredSensorDataStream) {

		// We need to get count of vehicle group by routeId and vehicleType
		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredSensorDataStream
				.mapToPair(sensor -> new Tuple2<>(new AggregateKey(sensor.getRouteId(), sensor.getVehicleType()), 1L))
				.reduceByKey((a, b) -> a + b);
		
		// Need to keep state for total count
		JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamWithStatePair = countDStreamPair
				.mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

		// Transform to dstream of TrafficData
		JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
		JavaDStream<TotalTrafficData> trafficDStream = countDStream.map(totalTrafficDataFunc);

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("routeId", "routeid");
		columnNameMappings.put("vehicleType", "vehicletype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "total_traffic",
				CassandraJavaUtil.mapToRow(TotalTrafficData.class, columnNameMappings)).saveToCassandra();
	}

	/**
	 * Method to get window traffic counts of different type of vehicles for each route.
	 * Window duration = 60 seconds and Slide interval = 60 seconds
	 * 
	 * @param filteredSensorDataStream sensor data stream
	 */
	public void processWindowTrafficData(JavaDStream<SensorData> filteredSensorDataStream) {

		// reduce by key and window (30 sec window and 10 sec slide).
		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredSensorDataStream
				.mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(60), Durations.seconds(60));

		// Transform to dstream of TrafficData
		JavaDStream<WindowTrafficData> trafficDStream = countDStreamPair.map(windowTrafficDataFunc);

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("routeId", "routeid");
		columnNameMappings.put("vehicleType", "vehicletype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream).writerBuilder("traffickeyspace", "window_traffic",
				CassandraJavaUtil.mapToRow(WindowTrafficData.class, columnNameMappings)).saveToCassandra();
	}

	/**
	 * Method to get the vehicles which are in radius of POI and their distance from POI.
	 * 
	 * @param nonFilteredSensorDataStream original sensor data stream
	 * @param broadcastPOIValues variable containing POI coordinates, route and vehicle types to monitor.
	 */
	public void processPOIData(JavaDStream<SensorData> nonFilteredSensorDataStream,Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues) {
		 
		
		
		// Filter by routeId,vehicleType and in POI range
		JavaDStream<SensorData> sensorDataStreamFiltered = nonFilteredSensorDataStream
				.filter(sensor -> (GeoDistanceCalculator.isInPOIRadius(Double.valueOf(sensor.getLatitude()),
								Double.valueOf(sensor.getLongitude()), broadcastPOIValues.value()._1().getLatitude(),
								broadcastPOIValues.value()._1().getLongitude(),
								broadcastPOIValues.value()._1().getRadius())));

		//sensor.getRouteId().equals(broadcastPOIValues.value()._2())
		//&& sensor.getVehicleType().contains(broadcastPOIValues.value()._3())
		//&&
		//sensorDataStreamFiltered.print();
		// pair with poi
		JavaPairDStream<SensorData, POIData> poiDStreamPair = sensorDataStreamFiltered
				.mapToPair(sensor -> new Tuple2<>(sensor, broadcastPOIValues.value()._1()));

		// Transform to dstream of POITrafficData
		JavaDStream<POITrafficData> trafficDStream = poiDStreamPair.map(poiTrafficDataFunc);

		// Map Cassandra table column
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("vehicleId", "vehicleid");
		columnNameMappings.put("distance", "distance");
		columnNameMappings.put("vehicleType", "vehicletype");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("latitude", "lat");
		columnNameMappings.put("longitude", "lng");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(trafficDStream)
				.writerBuilder("traffickeyspace", "poi_traffic",CassandraJavaUtil.mapToRow(POITrafficData.class, columnNameMappings))
				.withConstantTTL(120)//keeping data for 2 minutes
				.saveToCassandra();
	}
	
	//Function to get running sum by maintaining the state
	private static final Function3<AggregateKey, Optional<Long>, State<Long>,Tuple2<AggregateKey,Long>> totalSumFunc = (key,currentSum,state) -> {
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };
    
    //Function to create TotalTrafficData object from sensor data
    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = (tuple -> {
		logger.debug("Total Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value "+ tuple._2());
		TotalTrafficData trafficData = new TotalTrafficData();
		trafficData.setRouteId(tuple._1().getRouteId());
		trafficData.setVehicleType(tuple._1().getVehicleType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});
    
    //Function to create WindowTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
		logger.debug("Window Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType()+ " value " + tuple._2());
		WindowTrafficData trafficData = new WindowTrafficData();
		trafficData.setRouteId(tuple._1().getRouteId());
		trafficData.setVehicleType(tuple._1().getVehicleType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});
    
    //Function to create POITrafficData object from IoT data
    private static final Function<Tuple2<SensorData, POIData>, POITrafficData> poiTrafficDataFunc = (tuple -> {
		POITrafficData poiTraffic = new POITrafficData();
		poiTraffic.setVehicleId(tuple._1.getVehicleId());
		poiTraffic.setVehicleType(tuple._1.getVehicleType());
		poiTraffic.setTimeStamp(new Date());
		poiTraffic.setLatitude(Double.valueOf(tuple._1.getLatitude()));
		poiTraffic.setLongitude(Double.valueOf(tuple._1.getLongitude()));
		double distance = GeoDistanceCalculator.getDistance(Double.valueOf(tuple._1.getLatitude()).doubleValue(),
				Double.valueOf(tuple._1.getLongitude()).doubleValue(), tuple._2.getLatitude(), tuple._2.getLongitude());
		logger.debug("Distance for " + tuple._1.getLatitude() + "," + tuple._1.getLongitude() + ","+ tuple._2.getLatitude() + "," + tuple._2.getLongitude() + " = " + distance);
		poiTraffic.setDistance(distance);
		return poiTraffic;
	});

}
