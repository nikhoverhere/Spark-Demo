package com.nikh.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple1;
import scala.Tuple2;

public class TestConSpark {
	
	public static void main(String[] args) {
		
		List<Double> sample = new ArrayList<>();
		sample.add(45.7);
		sample.add(45.9);
		sample.add(145.89);
		sample.add(4789.678);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("TestConnection").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Function2<Double, Double, Double> reduceSumFunc = (accum, n) -> (accum + n);
		// The driver will send the func to each partition in rdd
		
		JavaRDD srdd = sc.parallelize(sample);
		
		JavaRDD<Double>sqrtrdd = srdd.map(rec -> Math.sqrt((double) rec));
		
		sqrtrdd.foreach(val -> System.out.println(val));
		
		//Or we can write as below:
		
		sqrtrdd.collect().forEach(System.out::println);
		//sqrtrdd.foreach(System.out::println);
		Double res = (Double) srdd.reduce(reduceSumFunc);
		
		//To check how many elements in rdd
		System.out.println(srdd.count());
		
		//Using Just Map and reduce
		JavaRDD<Integer> cnt = srdd.map(value -> 1);
		Integer cntr = cnt.reduce((a,b) -> a+b);
		
		System.out.println(cntr);
		
		System.out.println(res);
		//One of the limitation of reduce function is, the input functions input type and output type 
		//should be the same
		
		JavaRDD<squareCalc> sqrrdd = srdd.map(rec -> new squareCalc((Double) (rec)));
		//squareCalc = new squareCalc(8);
		
		/* Tuples*/
		Tuple2<Integer, Double> myTuple = new Tuple2<> (9, 5.5);
		JavaRDD<Tuple2<Integer, Double>> sqrtuple = srdd.map(rec -> new Tuple2<> (rec, Math.sqrt((double) rec)));
		sc.close();
		
//		Scanner scanner = new Scanner(System.in);
//		scanner.nextLine();
	}
}
