package com.nikh.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class TestJoins {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf =  new SparkConf().setAppName("Joins").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
		visits.add(new Tuple2<>(4, 18));
		visits.add(new Tuple2<>(6, 4));
		visits.add(new Tuple2<>(10, 9));
		
		List<Tuple2<Integer, String>> users = new ArrayList<>();
		users.add(new Tuple2<>(1, "John"));
		users.add(new Tuple2<>(2, "Bob"));
		users.add(new Tuple2<>(3, "Alan"));
		users.add(new Tuple2<>(4, "Doris"));
		users.add(new Tuple2<>(5, "Annabele"));
		users.add(new Tuple2<>(6, "Rafele"));
		
		JavaPairRDD<Integer, Integer> visitsRDD = sc.parallelizePairs(visits);
		JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);
		
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visitsRDD.join(usersRDD);
		
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRDDLeftOuter = visitsRDD.leftOuterJoin(usersRDD);
		
		//joinedRDDLeftOuter.foreach(x -> System.out.println(x._2._2.get().toUpperCase()));
		joinedRDDLeftOuter.foreach(x -> System.out.println(x._2._2.orElse("blank").toUpperCase()));
		sc.close();
	}
}
