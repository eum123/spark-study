package net.study.spark.operations.transformation;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapExample {
	public static void main(String... args) {

		JavaSparkContext sc = new JavaSparkContext("local", "FlatMap Example");

		JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello World", "Hi"));
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				
				 return Arrays.asList(line.split(" ")).iterator();
			}
			
		});

		System.out.println(words.count()); // 3 출력
		System.out.println(StringUtils.join(words.collect(), ",")); // Hello,World,Hi 출력
	}
}
