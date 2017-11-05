package kafkaAndstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import order.OrderInfo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cbw on 2017/11/3.
 */
public class ParserOrderMqBolt extends BaseRichBolt {

    private JedisPool pool;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxTotal(1000 * 100);
        config.setMaxWaitMillis(30);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        pool = new JedisPool(config, "127.0.0.1", 6379);
    }

    @Override
    public void execute(Tuple tuple) {
        Jedis jedis = pool.getResource();
        //获取kafkaSpout发送过来的数据，是一个json
        String string = new String((byte[]) tuple.getValue(0));
        OrderInfo orderInfo = new Gson().fromJson(string, OrderInfo.class);
        jedis.incrBy("totalAmount",orderInfo.getProductPrice());
        String bid =  getBubyProductId(orderInfo.getProductId(),"b");
        jedis.incrBy(bid+"Amount",orderInfo.getProductPrice());
        jedis.close();
    }

    private String getBubyProductId(String productId,String type) {
        // key:value
        // index:productID:info---->Map
        //  productId-----<各个业务线，各个品类，各个店铺，各个品牌，每个商品>
        Map<String,String> map =  new HashMap<>();
        map.put("b","3c");
        map.put("c","phone");
        map.put("s","121");
        map.put("p","iphone");
        return map.get(type);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
