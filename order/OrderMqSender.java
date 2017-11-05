package order;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by cbw on 2017/11/3.
 */
public class OrderMqSender {

    public static void main(String[] args) {
        String TOPIC = "orderMq";
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "shizhan1:9092,shizhan2:9092,shizhan3:9092");
        props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        for (int messageNo = 1; messageNo < 100000; messageNo++){
            producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "",new OrderInfo().random() ));
        }
    }

}
