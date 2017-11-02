package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cbw on 2017/10/29.
 */
public class MyCountBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<String,Integer> map = new HashMap<String,Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }

    public void execute(Tuple tuple) {

        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);

        if (map.containsKey(word)){

            Integer count = map.get(word);
            map.put(word,count+num);

        }else {

            map.put(word,num);

        }

        System.out.println("count"+map);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //不输出
    }
}
