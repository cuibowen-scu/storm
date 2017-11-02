package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by cbw on 2017/10/29.
 */
public class MySplitBolt extends BaseRichBolt {

    OutputCollector collector;

    //初始化
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    // 被storm框架不停循环调用 传入参数tuple
    public void execute(Tuple tuple) {

        String line = tuple.getString(0);

        String[] arrWords = line.split(" ");

        for (String word:arrWords){

            collector.emit(new Values(word,1));

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word","num"));

    }
}
