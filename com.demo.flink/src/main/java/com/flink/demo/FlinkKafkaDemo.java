package com.flink.demo;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkKafkaDemo {

	public static void main(String[] args) throws Exception {
		final Logger log = LoggerFactory.getLogger(FlinkKafkaDemo.class);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				"sd-bigdata-kafka2-60-165.idc.vip.com:9093,sd-bigdata-kafka2-60-171.idc.vip.com:9093,sd-bigdata-kafka2-60-187.idc.vip.com:9093,sd-bigdata-kafka2-60-193.idc.vip.com:9093,sd-bigdata-kafka2-60-199.idc.vip.com:9093");

		properties.setProperty("zookeeper.connect",
				"sd-bigdata-zookeeper6-61-181.idc.vip.com:2181,sd-bigdata-zookeeper6-61-182.idc.vip.com:2181,sd-bigdata-zookeeper6-61-183.idc.vip.com:2181/bigdata/kafka_cluster1");
		properties.setProperty("group.id", "storm-metric-persistent-flink-dxwang-test");
		properties.setProperty("auto.commit.interval.ms", "2000");

		DataStreamSource dataStream = env
				.addSource(new FlinkKafkaConsumer08("vrcp_pv_metrics_message", new SimpleStringSchema(), properties));
		log.info(dataStream.toString());
		env.execute("only-test-dxwang");

	}

}
