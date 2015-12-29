package StreamingKMeans;

import StreamingKMeans.util.Feature;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by ken on 11/17/2015.
 */
public class ClusteringSpout extends BaseRichSpout {

  private SpoutOutputCollector _collector;
  private String cloudID;
  private int sleepTime;
  private Random random;

  private Map stormConf;
  private final String cloudNameKey = "cloud-name";

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringSpout.class);

  public ClusteringSpout(boolean ackEnabled) {
    sleepTime = 2;
    random = new Random();
  }



  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    stormConf = conf;
    cloudID =  stormConf.get(cloudNameKey).toString();
    _collector = collector;
  }

  public void nextTuple() {

    Values value = new Values();
    double[] feature = new double[2];

    feature[0] = random.nextDouble();
    feature[1] = random.nextDouble();

    value.add(new Feature(feature, cloudID));

    _collector.emit(value);

    //sleep for X seconds
    try {
      Thread.sleep(sleepTime, 0);
    } catch(Exception e) { LOG.error(e.getMessage()); }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("feature"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return super.getComponentConfiguration();
  }
}
