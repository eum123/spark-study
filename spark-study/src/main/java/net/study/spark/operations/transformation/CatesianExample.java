package net.study.spark.operations.transformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CatesianExample {
	public static void main(String... args) {

		JavaSparkContext sc = new JavaSparkContext("local", "Union Example");

		JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1,2,3));
		
		JavaPairRDD<Integer, Integer> words = lines.cartesian(sc.parallelize(Arrays.asList(3,4,5)));
		
		System.out.println(words.count()); 
		System.out.println(StringUtils.join(words.collect(), ","));  
	}
}
