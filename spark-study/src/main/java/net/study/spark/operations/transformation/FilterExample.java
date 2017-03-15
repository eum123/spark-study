package net.study.spark.operations.transformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FilterExample {
	public static void main(String... args) {

		JavaSparkContext sc = new JavaSparkContext("local", "Filter Example");

		JavaRDD<String> lines = sc.parallelize(Arrays.asList("orange", "apple", "book"));
		
		JavaRDD<String> words = lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				
				if(line.equals("book")) {
					return false;
				}
				return true;
			}
			
		});
		

		System.out.println(words.count()); // 2 출력
		System.out.println(StringUtils.join(words.collect(), ",")); // orange, apple 출력
	}
}
