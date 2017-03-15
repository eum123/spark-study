package net.study.spark.operations.transformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DistinctExample {
	public static void main(String... args) {

		JavaSparkContext sc = new JavaSparkContext("local", "Distinct Example");

		JavaRDD<String> lines = sc.parallelize(Arrays.asList("orange", "apple", "orange"));
		
		JavaRDD<String> words = lines.distinct();
		
		System.out.println(words.count()); // 2 출력
		System.out.println(StringUtils.join(words.collect(), ",")); // orange, apple 출력
	}
}
