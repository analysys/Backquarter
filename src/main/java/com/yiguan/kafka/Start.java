package com.yiguan.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * 把kafka回传数据从阿里云同步到新集群的kafka <br>
 * 1、读取线上kafka数据<br>
 * 2、写入到新集群kafka队列<br>
 * 3、启动新集群kafka消费MR<br>
 * 4、在HDFS生成json<br>
 * 5、启动新集群HDFS的json消费MR<br>
 * 6、在HDFS生成4张表。<br>
 * @author Europe
 */
public class Start {
    public static AtomicInteger SDK201 = new AtomicInteger(0);
    public static AtomicInteger APP202 = new AtomicInteger(0);
    public static AtomicInteger APP300 = new AtomicInteger(0);
    public static AtomicInteger ALLAPP = new AtomicInteger(0);
    public static AtomicInteger SDK168 = new AtomicInteger(0);
    public static AtomicInteger APP200 = new AtomicInteger(0);
    public static AtomicInteger H5SDK = new AtomicInteger(0);
    public static AtomicInteger SDK2012 = new AtomicInteger(0);
    public static AtomicInteger H5SDKRT = new AtomicInteger(0);
    public static AtomicInteger APPRT = new AtomicInteger(0);

    public static void main(String[] args) {
        Map<String, String> topicGroupMap = new HashMap<String, String>();
        topicGroupMap.put("sdk_2_0_1_hdfs_8089_kafka", "kafka2kafka_sdk_2_0_1_group02_new");
        topicGroupMap.put("app_2_0_2", "kafka2kafka_app_2_0_2_group02_new");
        topicGroupMap.put("app_3_0", "kafka2kafka_app_3_0_group02_new");
        topicGroupMap.put("app_all_CASSANDRA", "kafka2kafka_app_all_CASSANDRA_group02_new");
        topicGroupMap.put("sdk_HDFS_168", "kafka2kafka_sdk_168_group02_new");
        topicGroupMap.put("app_2_0_HDFS", "kafka2kafka_app_2_0_group02_new");
        topicGroupMap.put("h5jssdk", "kafka2kafka_h5jssdk_group02_new");
        topicGroupMap.put("sdk_2_0_1_hdfs_8089_kafka_2", "kafka2kafka_sdk_2_0_1_2_group02_new");
        topicGroupMap.put("h5jssdk_real_time", "kafka2kafka_h5jssdk_real_time_group02_new");
        topicGroupMap.put("app_real_time", "kafka2kafka_app_real_time_group02_new");
        boolean isConsumer = false;
        int c = 1;
        if (args.length >= 1) {
            isConsumer = "c".equalsIgnoreCase(args[0]);
        }
        if (isConsumer) {
            int processCnt = 1;
            if (args.length >= 2) {
                processCnt = Integer.valueOf(args[1]);
            }
            for (int i = 0; i < 8; i++) {
                try {
                    if (processCnt == 2) {
                        new Thread(new Consumer("sdk_HDFS_168", "kafka2kafka_sdk_168_group02_new", Logger.getLogger("sdk_HDFS_168"))).start();
                        new Thread(new Consumer("app_2_0_HDFS", "kafka2kafka_app_2_0_group02_new", Logger.getLogger("app_2_0_HDFS"))).start();
                        new Thread(new Consumer("h5jssdk", "kafka2kafka_h5jssdk_group02_new", Logger.getLogger("h5jssdk"))).start();
                    } else if (processCnt == 3) {
                        String topicName = String.valueOf(args[2]);
                        if (topicName.equals("app_2_0_2")) {
                            new Thread(new App202Consumer(topicName, topicGroupMap.get(topicName), Logger.getLogger(topicName))).start();
                        } else {
                            new Thread(new Consumer(topicName, topicGroupMap.get(topicName), Logger.getLogger(topicName))).start();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                c++;
            }
        } else {
            int processCnt = 1;
            String index = "1";
            if (args.length >= 2) {
                processCnt = Integer.valueOf(args[1]);
            }
            if (args.length >= 4) {
            	index = args[3];
            }
            if (processCnt == 1) {
                try {
                    new Thread(new ProducerFromFile("app_2_0_HDFS", index)).start();
                    new Thread(new ProducerFromFile("app_2_0_2", index)).start();
                    new Thread(new ProducerFromFile("app_all_CASSANDRA", index)).start();
                    new Thread(new ProducerFromFile("sdk_HDFS_168", index)).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (processCnt == 3) {
            	try {
            		new ProducerFromFile(String.valueOf(args[2]), index).run();
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        }
    }
}
