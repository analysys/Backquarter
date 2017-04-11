package com.yiguan.kafka.util;

import com.analysys.common.util.PropertiesUtil;

/**
 * KafkaTool 配置项
 * @author GAO
 */
public class ProducerConstants {
	/*新集群*/
	public static final String KAFKA_BROKER = PropertiesUtil.getString("producer.kafka.broker");
	public static final String ZK_CONNECT = PropertiesUtil.getString("producer.zk");
	public static final String FILE_QUEUE_HOST = PropertiesUtil.getString("producer.read.file.queue.service");

	// 指定序列化处理类
	public static String serializerClass = "kafka.serializer.DefaultEncoder";
	// 指定分区处理类。默认kafka.producer.DefaultPartitioner，表通过key哈希到对应分区
	public static String partitionerClass = "kafka.producer.DefaultPartitioner";
	// 是否压缩，默认0表示不压缩，1表示用gzip压缩，2表示用snappy压缩
	public static String compressionCodec = "2";
	// 如果要压缩消息，这里指定哪些topic要压缩消息，默认empty，表示不压缩。
	// public static String compressedTopics = "kafka_test_ouzhou";
	public static String compressedTopics = "t08,test_sdk_gaochao";
	// 该属性表示你需要在消息被接收到的时候发送ack给发送者。以保证数据不丢失
	public static String acks = "1";
	// 默认“sync”表同步,"async"表异步.异步可以提高发送吞吐量
	public static String producerType = "async";
	// 在向producer发送ack之前,broker允许等待的最大时间 ,如果超时,broker将会向producer发送一个error
	// ACK.意味着上一次消息因为某种 # 原因未能成功(比如follower未能同步成功)
	public static String requestTimeoutMS = "10000";
	// 在async模式下,当message被缓存的时间超过此值后,将会批量发送给broker,默认为5000ms
	public static String queueBufferingMaxMS = "10000";
	public static String queueBufferingMaxMessages = "20000";
	// 异步 每次批次发送的数量 默认200
	public static String batchNumMessages = "500";
	public static String queueEnqueueTimeoutMS = "-1";
}
