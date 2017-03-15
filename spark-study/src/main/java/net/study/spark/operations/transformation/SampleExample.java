package net.study.spark.operations.transformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SampleExample {
	public static void main(String... args) {

		JavaSparkContext sc = new JavaSparkContext("local", "Sample Example");

		JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1,2,3,4,5));
		
		JavaRDD<Integer> words = lines.sample(true, 1);
		
		System.out.println(words.count()); 
		System.out.println(StringUtils.join(words.collect(), ","));  
	}
}
