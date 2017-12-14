package org.test.spark.app.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class TestSparkStreamingKafka {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with n working threads and batch
		// interval of 30 seconds
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "test_group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		//Kafka server should be up and running and there should be a topic by this name
		Collection<String> topics = Arrays.asList("testSparkStreamTopic");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<String> outputStream = stream.map(new Function<ConsumerRecord<String, String>, String>() {

			private static final long serialVersionUID = 990029841113738302L;

			@Override
			public String call(ConsumerRecord<String, String> input) throws Exception {
				int partition = input.partition();
				String topicName = input.topic();
				long offset = input.offset();
				String partitionStr = Integer.toString(partition);
				String offsetStr = Long.toString(offset);
				String combinedName = "topic is " + topicName + ":" + "partition is " + partitionStr + ":"
						+ "offset is " + offsetStr + ":" + "value is " + input.value();
				System.out.println("combinedName: " + combinedName);
				return combinedName;

			}
		});

		outputStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

			private static final long serialVersionUID = 6120568359089902725L;

			@Override
			public void call(JavaRDD<String> inputRDD, Time arg1) throws Exception {
				SparkSession spark = SparkSession.builder().config(inputRDD.context().conf()).getOrCreate();
				SQLContext sqlContext = spark.sqlContext();
				JavaRDD<Row> outputRDD = inputRDD.map(new Function<String, Row>() {

					private static final long serialVersionUID = -3503727288521413141L;

					@Override
					public Row call(String arg0) throws Exception {

						return RowFactory.create(arg0);
					}

				});
				List<StructField> fields = new ArrayList<StructField>();
				fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
				StructType schema = DataTypes.createStructType(fields);
				Dataset<Row> kafkaDF = sqlContext.createDataFrame(outputRDD.rdd(), schema);
				kafkaDF.printSchema();
				kafkaDF.show();
			}

		});

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}

}
