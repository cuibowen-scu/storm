package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by cbw on 2017/10/29.
 */
public class WordCountTopologMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        //准备一个topologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        topologyBuilder.setBolt("mybolt1",new MySplitBolt(),10).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("mybolt2",new MyCountBolt(),2).fieldsGrouping("mybolt1",new Fields("word"));

        //创建一个config用来指定当前topology需要的worker数量
        Config config = new Config();
        config.setNumWorkers(2);

        //提交任务--本地模式或集群模式
        StormSubmitter.submitTopology("mywordcount",config,topologyBuilder.createTopology());

       /* LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount",config,topologyBuilder.createTopology());*/


    }
}











