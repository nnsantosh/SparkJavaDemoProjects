package org.test.spark.app;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

//Needs one argument and that will be the path of the input text file having few lines
public class WordCount {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		//SparkConf conf = new SparkConf().set("spark.sql.warehouse.dir", "file:///C:/apache_spark_distribution/inputTestDataFiles");
		//SparkContext scs = new SparkContext(conf);
		//JavaSparkContext sc = new JavaSparkContext(scs);
		SparkSession session = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		if(args.length == 0){
			System.out.println("missing argument input file path");
			System.exit(1);
		}
		String inputTextFilePath = args[0];
		System.out.println("WordCount.main() inputTextFilePath: "+inputTextFilePath);
		if (StringUtils.isNotBlank(args[0])) {
			JavaRDD<String> lines = sc.textFile(inputTextFilePath);
			JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				private static final long serialVersionUID = -9064757570834006983L;

				public Iterator<String> call(String line) throws Exception {
					Iterator<String> iter = Arrays.asList(line.split(" ")).iterator();
					return iter;
				}
			});

			JavaPairRDD<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = 132846150178206689L;

				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {

					return new Tuple2<String, Integer>(word, 1);
				}
			});

			JavaPairRDD<String, Integer> wordCounts = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {

				private static final long serialVersionUID = -2804037352668598070L;

				@Override
				public Integer call(Integer arg0, Integer arg1) throws Exception {
					// TODO Auto-generated method stub
					return arg0 + arg1;
				}
			});
			wordCounts.sortByKey();
			List<Tuple2<String, Integer>> list = wordCounts.collect();
			for (Tuple2<String, Integer> tp : list) {
				System.out.println("word : " + tp._1() + " count: " + tp._2());
			}

			Dataset<String> wordDS = session.createDataset(words.rdd(), Encoders.STRING());
			wordDS.printSchema();
			wordDS.show();
		}else{
			System.out.println("input file path is missing");
			System.exit(1);
		}
		sc.close();
	}

}
