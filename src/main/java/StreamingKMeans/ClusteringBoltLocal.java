package StreamingKMeans;

import StreamingKMeans.util.Feature;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by ken on 11/17/2015.
 */
public class ClusteringBoltLocal extends BaseRichBolt {

  private FurthestPointKMeans kmeans;
  private OutputCollector _outputCollector;
  private Map stormConf;
  private String cloudID;
  private final String cloudNameKey = "cloud-name";

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringBoltLocal.class);

  public ClusteringBoltLocal(int clusterGroup, double errorRate, long expireTime, boolean ackEnabled) {

    kmeans = new FurthestPointKMeans(clusterGroup, errorRate, expireTime);
    kmeans.reset();
  }

  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    stormConf = conf;
    cloudID =  stormConf.get(cloudNameKey).toString();
    _outputCollector = collector;
  }

  public void execute(Tuple input) {
    Feature feature = (Feature) input.getValueByField("feature");

    boolean update = kmeans.update(feature);

    if(update) {
      List<Feature> centroids = kmeans.getCentroids();
      _outputCollector.emit(input, new Values(centroids, cloudID));
      StringBuilder builder = new StringBuilder();
      builder.append("new centroids\n");
      for(Feature f : centroids)
        builder.append(f.value + "\n");
      LOG.info(builder.toString());
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("featureList", "CloudID"));
  }
}
