package StreamingKMeans;

import StreamingKMeans.util.Feature;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by ken on 11/17/2015.
 */
public class ClusteringBoltGlobal extends BaseBasicBolt {

  private FurthestPointKMeans kMeans;

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringBoltGlobal.class);

  public ClusteringBoltGlobal(int clusterGroup, double errorRate, long expireTime, boolean ackEnabled) {

    kMeans = new FurthestPointKMeans(clusterGroup, errorRate, expireTime);
    kMeans.reset();
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    List<Feature> localFeatures = (List<Feature>) input.getValueByField("featureList");
    String senderLocation = input.getStringByField("CloudID");

    kMeans.clearFeatureFromLocation(senderLocation);

    for(Feature newFeature : localFeatures)
      kMeans.update(newFeature);

    List<Feature> globalCentroids = kMeans.getCentroids();
    StringBuilder builder = new StringBuilder();
    builder.append("new centroids\n");
    for(Feature f : globalCentroids)
      builder.append(f.value + "\n");
    LOG.info(builder.toString());
    //collector.emit(new Values(input.getValue(0), centroids));

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cloudID", "centroids"));
  }
}
