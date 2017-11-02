package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

/**
 * Created by cbw on 2017/11/1.
 */
public class MyLogPartitioner implements Partitioner {

    private static Logger logger = Logger.getLogger(MyLogPartitioner.class);

    public MyLogPartitioner(VerifiableProperties props) {
    }

    public int partition(Object obj, int numPartitions) {
        return Integer.parseInt(obj.toString())%numPartitions;
    }
}
