package com.yiguan.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import com.yiguan.kafka.service.TimeService;
import com.yiguan.kafka.util.Constants;
import com.yiguan.kafka.util.OZBase64;
import com.yiguan.kafka.util.ProducerConstants;

/**
 * 读取文件,向IDC的Kafka写入数据
 * @author Administrator
 */
public class ProducerFromFile implements Runnable {
	public static ProducerConfig config;
	private Logger logger = Logger.getLogger("ProducerLogs");
	private Pattern pattern = Pattern.compile("[0-9]*");
	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	private String workSpace;
	private String index = "1";
	private Integer cache_num = 1000;

	static {
		Properties pro = new Properties();
		pro.put("metadata.broker.list", ProducerConstants.KAFKA_BROKER);
		pro.put("zookeeper.connect", ProducerConstants.ZK_CONNECT);
		pro.put("serializer.class", ProducerConstants.serializerClass);
		pro.put("partitioner.class", ProducerConstants.partitionerClass);
		pro.put("request.required.acks", ProducerConstants.acks);
		pro.put("compression.codec", ProducerConstants.compressionCodec);
		pro.put("compressed.topics", ProducerConstants.compressedTopics);
		pro.put("request.timeout.ms", ProducerConstants.requestTimeoutMS);
		pro.put("queue.buffering.max.ms", ProducerConstants.queueBufferingMaxMS);
		pro.put("queue.buffering.max.messages", ProducerConstants.queueBufferingMaxMessages);
		pro.put("batch.num.messages", ProducerConstants.batchNumMessages);
		pro.put("queue.enqueue.timeout.ms", ProducerConstants.queueEnqueueTimeoutMS);
		config = new ProducerConfig(pro);
	}

	@SuppressWarnings("rawtypes")
	public kafka.javaapi.producer.Producer producer = new kafka.javaapi.producer.Producer(config);
	public KeyedMessage<byte[], byte[]> data;
	public int MSCOUNT = 0;
	private String topicName;
	Map<String, AtomicInteger> map = new HashMap<String, AtomicInteger>();

	public ProducerFromFile(String topicName, String index) {
		this.topicName = topicName;
		this.index = index;
		this.workSpace = Constants.DEAL_DATA_FILE_DIR + "/" + topicName + "/" + this.index;
		File folder = new File(this.workSpace);
		if(!folder.exists())
			folder.mkdirs();
		map.put(topicName, new AtomicInteger(0));
	}

	@SuppressWarnings("rawtypes")
	private KeyedMessage getMsg(String topic, String key, byte[] bytes) {
		return new KeyedMessage<byte[], byte[]>(topic, key.getBytes(), bytes);
	}

	public void close() {
		producer.close();
	}

	/**
	 * 获取待处理文件
	 * @param topic
	 * @return
	 */
	private String getCanReadFile(String topic) {
		String targetFilePath = "";
		//先轮询自己的工作目录
		if(map.get(topicName).get() == 0) {
			File myFolder = new File(this.workSpace);
			File[] myFiles = myFolder.listFiles();
			if(myFiles.length > 0){
				for(File oldFile : myFiles){
					moveFile(oldFile.getAbsolutePath(), Constants.READ_DATA_FILE_DIR);
				}
			}
		}
		//再去待处理目录抓取文件(生成时间排序)
		File folder = new File(Constants.READ_DATA_FILE_DIR);
		File[] files = folder.listFiles();
		if(files != null && files.length > 1){
			try {
				Arrays.sort(files, new Comparator<File>() {
					@Override
					public int compare(File f1, File f2) {
						long diff = f1.lastModified() - f2.lastModified();
						if (diff > 0) {
							return 1;
						} else if (diff == 0) {
							return 0;
						} else {
							return -1;
						}
					}
				});
			} catch (Exception e) {}
		}
		for(File file : files) {
			if(file.isDirectory())
				continue;
			String topicName = topic + ".";
			if(file.getName().startsWith(topicName)){
				boolean success = moveFile(file.getAbsolutePath(), this.workSpace);
				if(success){
					targetFilePath = this.workSpace + "/" + file.getName();
					break; 
				}
			}
		}
		return targetFilePath;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void send(List<KeyedMessage> ls, List<String> tmpTopicName, int lineCount, File file){
		int errNum = 0;
		for (int i = 0; i < 5; i++) {
			try {
				producer.send(ls);
				System.out.println(String.format("read %s|%s\t%s|%s\t%s\t%s", tmpTopicName, (topicName), lineCount, map.get(topicName).get(), file, TimeService.getInstance().getCurrentTimeYYYYMMDDHHMMSS()));
				return;
			} catch (Exception e) {
				errNum++;
				int msg_length = 0;
				try {
					for(KeyedMessage m : ls)
						msg_length += ((byte[])m.message()).length;
				} catch (Exception e2) {}
				System.out.println(String.format("readerr %s\t%s|%s\t%s|%s", tmpTopicName, lineCount, msg_length, file, TimeService.getInstance().getCurrentTimeYYYYMMDDHHMMSS()));
			}
			if (errNum == 3) {
				try {
					producer.close();
					producer = new kafka.javaapi.producer.Producer(config);
					producer.send(ls);
					return;
				} catch (Exception e) {
					System.out.println("###" + e.getMessage());
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void run() {
		try {
			String configFileName = "";
			byte[] message = null;
			String lineTxt = null;
			List<KeyedMessage> ls = null;
			int lineCount = 0;
			InputStreamReader read = null;
			BufferedReader bufferedReader = null;
			while (true) {
				configFileName = getCanReadFile(topicName);
				if(configFileName != null && !"".equals(configFileName)){
					File _file = new File(configFileName);
					if (!(_file.isFile() && _file.exists())) { // 判断文件是否存在
						Thread.sleep(60000L);
						continue;
					}
				} else {
					System.out.println(TimeService.getInstance().getCurrentTimeYYYYMMDDHHMMSS() + "   " + topicName + "=== NoFile to deal, wait 1m...");
					Thread.sleep(60000L);
					continue;
				}
				File file = new File(configFileName);
				if (file.isFile() && file.exists()) {
					try {
						System.out.println(topicName + "===Read datafile:" + (configFileName) + " " + (file.isFile() && file.exists()));
						long startTime = System.currentTimeMillis();
						read = new InputStreamReader(new FileInputStream(file));// 考虑到编码格式
						bufferedReader = new BufferedReader(read);
						lineCount = 0;
						ls = new ArrayList<KeyedMessage>(cache_num);
						List<String> tmpTopicName = getTopicNameWithTimestamp(topicName, file.getName());
						while ((lineTxt = bufferedReader.readLine()) != null) {
							lineCount++;
							map.get(topicName).addAndGet(1);
							try {
								message = OZBase64.decode(lineTxt);
								for (String tmp : tmpTopicName) {
									ls.add(getMsg(tmp, tmp, message));
								}
								if (ls.size() >= cache_num) {
									send(ls, tmpTopicName, lineCount, file);
									ls = new ArrayList<KeyedMessage>(cache_num);
								}
							} catch (UnsupportedEncodingException e) {
								logger.error(e.getMessage(), e);
							}
						}
						if (ls.size() > 0) {
							send(ls, tmpTopicName, lineCount, file);
							ls = new ArrayList<KeyedMessage>(1);
						}
						System.out.println(topicName + "===Read datafile:" + (configFileName) + " use time:" + (System.currentTimeMillis() - startTime) + " linecnt:" + lineCount);
					} catch (Exception e) {
						e.printStackTrace();
						logger.error(e);
					} finally {
						if (bufferedReader != null) {
							bufferedReader.close();
							bufferedReader = null;
						}
						if (read != null) {
							read.close();
							read = null;
						}
					}
					processComplete(topicName, new File(configFileName));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

	/**
	 * 获取队列名称(根据文件名称)
	 * @param topicName
	 * @param fileName
	 * @return
	 */
	private List<String> getTopicNameWithTimestamp(String topicName, String fileName) {
		boolean isEnd = System.currentTimeMillis() > TimeService.getInstance().getEndTime();
		int cnt = isEnd ? 1 : 2;
		ArrayList<String> topics = new ArrayList<String>(cnt);
		if (!isEnd) {
			topics.add(topicName);
		}
		try {
			String topicDay = fileName.split("\\.")[2];
			format.parse(topicDay);
			if (topicDay != null && topicDay.length() == 8 && pattern.matcher(topicDay).matches()) {
				topics.add(new StringBuilder().append(topicName).append("_").append(topicDay).toString());
			} else {
				topics.add(new StringBuilder().append(topicName).append("_").append(TimeService.getInstance().getCurrentTimeYYYYMMDD()).toString());
			}
		} catch (Exception e) {
			topics.add(new StringBuilder().append(topicName).append("_").append(TimeService.getInstance().getCurrentTimeYYYYMMDD()).toString());
		}
		return topics;
	}
	
	/**
	 * 备份文件
	 * @param topic
	 * @param file
	 */
	private void processComplete(String topic, File file) {
		String fileDay = "";
		try {
			fileDay = file.getName().split("\\.")[2];
			format.parse(fileDay);
			if (fileDay == null || fileDay.length() != 8 || !pattern.matcher(fileDay).matches()) {
				fileDay = new SimpleDateFormat("yyyyMMdd").format(new Date());
			}
		} catch (Exception e) {
			fileDay = new SimpleDateFormat("yyyyMMdd").format(new Date());
		}
		String bakFullDir = getBakDir(fileDay, topic);
		moveFile(file.getAbsolutePath(), bakFullDir);
	}
	
	/**
	 * 移动文件
	 * @param srcFile
	 * @param destPath
	 * @return
	 */
	private boolean moveFile(String srcFile, String destPath){
		File fileFrom = new File(srcFile);
		if(!fileFrom.exists())
			return false;
		File toDir = new File(destPath);
		try {
			boolean success = fileFrom.renameTo(new File(toDir, fileFrom.getName()));
			return success;
		} catch (Exception e) {
			return false;
		}
	}
	
	private String getBakDir(String fileDay, String topic){
		String backUpDir = Constants.BACKUP_DATA_FILE_DIR;
		String bakFullDir = backUpDir + "/" + fileDay + "/" + topic;
		File folder = new File(bakFullDir);
		if(!folder.exists())
			folder.mkdirs();
		return bakFullDir;
	}

}
