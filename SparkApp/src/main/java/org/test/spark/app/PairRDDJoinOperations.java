package org.test.spark.app;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class PairRDDJoinOperations {
	
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("PairRDDJoinOperations").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Create PairRDD 
		Tuple2<String,String> tp1 = new Tuple2<String,String>("Los Angeles", "lax Airport");
		Tuple2<String,String> tp2 = new Tuple2<String,String>("San Francisco", "sfo Airport");
		Tuple2<String,String> tp3 = new Tuple2<String,String>("Las Vegas", "vegas Airport");
		List<Tuple2<String,String>> cityAirportTupleList = new ArrayList<Tuple2<String,String>>();
		cityAirportTupleList.add(tp1);
		cityAirportTupleList.add(tp2);
		cityAirportTupleList.add(tp3);
		
		Tuple2<String,String> tp11 = new Tuple2<String,String>("Los Angeles", "Getty museum");
		Tuple2<String,String> tp21 = new Tuple2<String,String>("San Francisco","Palace of fine arts");
		Tuple2<String,String> tp31 = new Tuple2<String,String>("New York","Madam Tussads");
		List<Tuple2<String,String>> cityAttractionTupleList = new ArrayList<Tuple2<String,String>>();
		cityAttractionTupleList.add(tp11);
		cityAttractionTupleList.add(tp21);
		cityAttractionTupleList.add(tp31);
		
		JavaPairRDD<String,String> cityAirportRDD = sc.parallelizePairs(cityAirportTupleList);
		JavaPairRDD<String,String> cityAttractionRDD = sc.parallelizePairs(cityAttractionTupleList);
		
		JavaPairRDD<String, Tuple2<String, String>> innerJoinRDD = cityAirportRDD.join(cityAttractionRDD);
		innerJoinRDD.saveAsTextFile("output/innerJoinResults.txt");
		
		JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinRDD = cityAirportRDD.leftOuterJoin(cityAttractionRDD);
		leftOuterJoinRDD.saveAsTextFile("output/leftOuterJoinResults.txt");
		
		JavaPairRDD<String, Tuple2<Optional<String>, String>> rightOuterJoinRDD = cityAirportRDD.rightOuterJoin(cityAttractionRDD);
		rightOuterJoinRDD.saveAsTextFile("output/rightOuterJoinResults.txt");
		
		JavaPairRDD<String,Tuple2<Optional<String>,Optional<String>>> fullJoinRDD = cityAirportRDD.fullOuterJoin(cityAttractionRDD);
		fullJoinRDD.saveAsTextFile("output/fullOuterJoinResults.txt");
		
		sc.close();
	}

}
