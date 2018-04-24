package com.flink.demo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PriceCepDemo {

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Price> input = env.fromElements(new Price("mk", 1000l), new Price("cache", 800l),
				new Price("jackjson", 700));

		Pattern<Price, ?> pattern = Pattern.<Price> begin("filter-001").where(new IterativeCondition<Price>() {

			@Override
			public boolean filter(Price value, Context<Price> ctx) throws Exception {
				return value.getPrice() > 750;
			}
		});

		DataStream<Double> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Price, Double>() {

			public Double select(Map<String, List<Price>> values) throws Exception {
				Collection<List<Price>> prices = values.values();
				if (prices == null) {
					return 0.0;
				}
				double sum = 0.0;
				for (List<Price> ps : prices) {
					for (Price p : ps) {
						sum += p.getPrice();
					}
				}
				return sum;
			}
		});

		result.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
