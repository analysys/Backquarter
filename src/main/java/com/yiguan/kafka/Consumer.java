package com.yiguan.kafka;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.log4j.Logger;

import com.yiguan.kafka.service.TimeService;
import com.yiguan.kafka.util.Constants;
import com.yiguan.kafka.util.OZBase64;

/**
 * 这个线程只负责读取卡夫卡中的数据
 * @author Europe
 */
public class Consumer implements Runnable {
	private final ConsumerConnector consumer;
	private final String topic;
	private String group;
	private Logger logger;
	private Logger consumerLogger = Logger.getLogger("ConsumeLogs");
	private Map<String, AtomicInteger> map = new HashMap<String, AtomicInteger>();

	public Consumer(String topic, String group, Logger logger) throws Exception {
		this.topic = topic + "_" + TimeService.getInstance().getCurrentTimeYYYYMMDD();
		this.logger = logger;
		this.group = group;
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		map.put("sdk_2_0_1_hdfs_8089_kafka", Start.SDK201);
		map.put("app_2_0_2", Start.APP202);
		map.put("app_3_0", Start.APP300);
		map.put("app_all_CASSANDRA", Start.ALLAPP);
		map.put("sdk_HDFS_168", Start.SDK168);
		map.put("app_2_0_HDFS", Start.APP200);
		map.put("h5jssdk", Start.H5SDK);
		map.put("sdk_2_0_1_hdfs_8089_kafka_2", Start.SDK2012);
		map.put(this.topic, new AtomicInteger(0));
	}

	/*
	 * 从kafka读取用
	 */
	private ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", Constants.ZK_CONNECT);
		props.put("group.id", group);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("fetch.message.max.bytes", "104857400");
		props.put("auto.offset.reset", "smallest");
		return new ConsumerConfig(props);
	}

	public void run() {
		readKafak();
	}

	public void readKafak() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		// 获取卡夫卡中的数据
		int count = 0;
		while (true) {
			String[] files = getFileLists();
			if (files.length > 50) {
				System.out.println(topic + " i will sleep times:" + (1000 * 60 * 5L));
				try {
					Thread.sleep(1000 * 60 * 5L);
					continue;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			while (it.hasNext()) {
				byte[] dataByte = it.next().message();
				try {
					logger.info(OZBase64.encode(dataByte));
					count++;
					if (count % 50000 == 0) {
						if (getFileLists().length > 50) {
							count = 0;
							break;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				logCnt(topic);
			}
		}
	}

	private String[] getFileLists() {
		File file = new File(Constants.WRITE_DATA_FILE_DIR);
		String[] files = file.list();
		return files;
	}

	private void logCnt(String topic) {
		TimeService service = TimeService.getInstance();
		long currentTime = System.currentTimeMillis();
		AtomicInteger counter = map.get(topic);
		synchronized (map) {
			if (!(currentTime >= service.getCurrentTimeHourLowerLimit() && currentTime < service.getCurrentTimeHourUpperLimit())) {
				synchronized (map) {
					if (!(currentTime >= service.getCurrentTimeHourLowerLimit() && currentTime < service.getCurrentTimeHourUpperLimit())) {
						consumerLogger.info(String.format("H\t%s\t%s\t%s", topic, counter.get(), service.getCurrentTimeYYYYMMDDHHMMSS()));
						counter.set(0);
						service.resetHourLimits();
					}
				}
			}
		}
		synchronized (map) {
			if (!(currentTime >= service.getCurrentTimeDayLowerLimit() && currentTime < service.getCurrentTimeDayUpperLimit())) {
				synchronized (map) {
					if (!(currentTime >= service.getCurrentTimeDayLowerLimit() && currentTime < service.getCurrentTimeDayUpperLimit())) {
						consumerLogger.info(String.format("D\t%s\t%s\t%s", topic, counter.get(), service.getCurrentTimeYYYYMMDDHHMMSS()));
						counter.set(0);
						service.resetDayLimits();
					}
				}
			}
		}
		counter.addAndGet(1);
		if (counter.get() % 1000 == 0) {
			System.out.println(String.format("%s\t%s\t%s", topic, counter.get(), service.getCurrentTimeHourUpperLimit()));
		}
	}
}
