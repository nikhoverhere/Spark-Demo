package com.nikh.spark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;

public class ReadFromFile {
	
	public static void main(String[] args) {
		
		Set<String> borings = new HashSet<String>();
		borings.add("this");
		borings.add("running");
		borings.add("docker");
		borings.add("tame");
		borings.add("spring");
		borings.add("microservice");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("TestConnection").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> testRead = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaPairRDD<String, Long> filteredEle = testRead.map(rec -> rec.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
					.filter(sentance -> sentance.trim().length() > 0)
					.flatMap(word -> Arrays.asList(word.split(" ")).iterator())
					.filter(sentance -> sentance.trim().length() > 0)
					.filter(word -> borings.contains(word))
					.mapToPair(word -> new Tuple2<String, Long> (word, 1L))
					.reduceByKey((x,y) -> x + y)
					.sortByKey();
		
		JavaPairRDD<Long, String> filteredEleSwitched = filteredEle.mapToPair(x -> new Tuple2<Long, String> (x._2, x._1))
														.sortByKey();
		
		List<Tuple2<String, Long>> elements = filteredEle.take(50);
		elements.forEach(x -> System.out.println(x));
		
		List<Tuple2<Long, String>> elementsSwitch = filteredEleSwitched.take(50);
		elementsSwitch.forEach(x -> System.out.println(x));
		/*testRead.flatMap(val -> Arrays.asList(val.split(" ")).iterator())
				.foreach(printVal -> System.out.println(printVal)); */
		
//		You cannot apply foreach on a sorted rdd w/o assigning to a List
//		above is the correct way
//		Also instead of foreach use take or anyother fn to fetch correct results
		//take(10) returns top 10 esults accross all partitions regarless of the partition its fetcing the 
		//data, but thats not the case with for each
		
//Aggregate operations and collect() is done in Driver
		sc.close();
	}

}
