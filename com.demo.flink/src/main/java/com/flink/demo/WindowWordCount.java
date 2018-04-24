package com.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

	// ./flink run -m yarn-cluster -d -yn 1 -yjm 4096 -ys 1 -ytm 4096 -ynm demo /home/apps/software/dxwang/com.demo.flink-0.0.1-SNAPSHOT.jar
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setParallelism(parallelism)

		DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("10.208.61.147", 1212)// 10.208.73.126
				.flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(10)).sum(1);
		dataStream.print();
		env.execute("window-word-count");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -7828081532369184348L;

		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

}
