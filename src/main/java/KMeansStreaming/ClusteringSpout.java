package KMeansStreaming;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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
  private Values value;
  private Map stormConf;
  private final String cloudNameKey = "cloud-name";
  double[] feature;

  public ClusteringSpout(boolean ackEnabled) {
    sleepTime = 2;
    random = new Random();
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cloudID", "feature"));
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    stormConf = conf;
    cloudID =  stormConf.get(cloudNameKey).toString();
    _collector = collector;
  }

  public void nextTuple() {

    value = new Values();
    feature = new double[2];

    feature[0] = random.nextDouble();
    feature[1] = random.nextDouble();

    value.add(cloudID);
    value.add(feature);

    _collector.emit(value);

    //sleep for 2 seconds
    try {
      Thread.sleep(sleepTime, 0);
    } catch(Exception e) { }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return super.getComponentConfiguration();
  }
}
