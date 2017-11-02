package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by cbw on 2017/11/1.
 */
public class KafkaConsumerSimple implements Runnable{

    public String title;
    public KafkaStream<byte[], byte[]> stream;

    public KafkaConsumerSimple(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }

    public void run() {
        System.out.println("开始运行"+title);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()){
            MessageAndMetadata<byte[], byte[]> data = iterator.next();
            String topic = data.topic();
            String msg = new String(data.message());
            long offset = data.offset();
            int partition = data.partition();
            System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, msg));
        }
        System.out.println(title+"exit");
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("group.id", "dashujujiagoushi");
        props.put("zookeeper.connect", "shizhan1:2181,shizhan2:2181,shizhan3:2181");
        props.put("auto.offset.reset", "largest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig config = new ConsumerConfig(props);
        String topic1 = "orderMq";
        String topic2 = "paymentMq";
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic1,4);
        topicCountMap.put(topic2,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConn.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic1);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i=0;i<streams.size();i++){
            executor.execute(new KafkaConsumerSimple("消费者"+i+1,streams.get(i)));
        }

    }
}
