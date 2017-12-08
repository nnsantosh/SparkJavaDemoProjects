To run spark programs on windows:
1. Download latest spark distribution from https://spark.apache.org/downloads.html
spark-2.2.0-bin-hadoop2.7.gz
2. Extract the folder spark-2.2.0-bin-hadoop2.7 and place it in some directory in C drive
Ex: C:\apache_spark_distribution
3. Build your application jar file with standalone spark programs
Ex: SparkApp-0.0.1-SNAPSHOT-jar-with-dependencies.jar
4. To submit spark job in windows machine go to the bin directory present inside the spark-2.2.0-bin-hadoop2.7 folder in command prompt
Ex: WordCount program needs input text file path as an argument
spark-submit --class org.test.spark.app.WordCount --master local[*] C:\apache_spark_distribution\SparkApp-0.0.1-SNAPSHOT-jar-with-dependencies.jar file:///C:\apache_spark_distribution\inputTestDataFiles\word_count.txt