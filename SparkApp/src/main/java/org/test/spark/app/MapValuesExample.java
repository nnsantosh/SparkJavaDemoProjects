package org.test.spark.app;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class MapValuesExample {
	
	public static void main(String[] args){
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		// SparkConf conf = new SparkConf().set("spark.sql.warehouse.dir",
		// "file:///C:/apache_spark_distribution/inputTestDataFiles");
		// SparkContext scs = new SparkContext(conf);
		// JavaSparkContext sc = new JavaSparkContext(scs);
		SparkSession session = SparkSession.builder().appName("MapValuesExample").master("local[*]").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		//Create PairRDD using tuple list and use mapValues
				Tuple2<String,String> tp1 = new Tuple2<String,String>("Los Angeles", "lax Airport");
				Tuple2<String,String> tp2 = new Tuple2<String,String>("San Francisco", "sfo Airport");
				List<Tuple2<String,String>> cityAirportTupleList = new ArrayList<Tuple2<String,String>>();
				cityAirportTupleList.add(tp1);
				cityAirportTupleList.add(tp2);
				
				JavaPairRDD<String,String> cityAirportPairRDD = sc.parallelizePairs(cityAirportTupleList);
				JavaPairRDD<String,String> cityAirportUpperPairRDD = cityAirportPairRDD.mapValues(new Function<String, String>(){

					
					private static final long serialVersionUID = -4567824686212517330L;

					@Override
					public String call(String airportName) throws Exception {
						// TODO Auto-generated method stub
						return airportName.toUpperCase();
					}
					
				});
				List<Tuple2<String, String>> airportList = cityAirportUpperPairRDD.collect();
				for(Tuple2<String,String> tp: airportList){
					System.out.println("city Name: "+tp._1() + " airport Name: "+tp._2());
				}
				
				sc.close();
		
	}

}
