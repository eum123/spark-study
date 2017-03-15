package net.study.spark.operations.transformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class MapExample {
	public static void main(String... args) {
		
		JavaSparkContext sc = new JavaSparkContext("local", "Map Example");

		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer x) {
				return x * x;
			}
		});

		System.out.println(StringUtils.join(result.collect(), ",")); // 1, 4, 9,
																		// 16 출력
	}
	
}
