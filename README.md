# Kafka Message Synchronization (data_sync)

![](process.png)


同步Ucloud上的Kafka消息到IDC机房：消费者消费Ucloud上Kafka集群的消息，生成文件，实时同步文件至IDC机房。生产者实时监控文件，解析文件发送至IDC的Kafka集群。

------

## 说明
 - src/java: 程序源码
 - src/resources: 配置文件
 - Consumer.java: 消费者
 - ProducerFromFile.java: 生产者
