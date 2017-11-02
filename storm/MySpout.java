package storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by cbw on 2017/10/29.
 */
public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;

    //初始化方法
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;

    }

    //storm 框架在不停的调用 nextTuple()
    public void nextTuple() {

        collector.emit(new Values("i am lilei love hanmeimei"));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("love"));

    }
}
