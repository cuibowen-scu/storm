package kafkaAndstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by cbw on 2017/11/3.
 */
public class KafkaAndStormTopologyMain {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout",new KafkaSpout(new SpoutConfig(new ZkHosts("shizhan1:2181,shizhan2:2181,shizhan3:2181"),"orderMq","/myKafka","kafkaSpout")),1);
        topologyBuilder.setBolt("mybolt1",new ParserOrderMqBolt(),1).shuffleGrouping("kafkaSpout");
        Config config = new Config();
        config.setNumWorkers(2);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount", config, topologyBuilder.createTopology());
    }
}
