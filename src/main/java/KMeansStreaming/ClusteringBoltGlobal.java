package KMeansStreaming;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by ken on 11/17/2015.
 */
public class ClusteringBoltGlobal extends BaseBasicBolt {

  private FurthestPointKMeans kmeans;

  public ClusteringBoltGlobal(int clusterGroup, boolean ackEnabled) {

    kmeans = new FurthestPointKMeans(clusterGroup, 0.2);
    kmeans.reset();
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    String location = input.getString(0);
    double[][] newFeatures = (double[][]) input.getValue(1);

    if(kmeans.isLocationExist(location))
      kmeans.clearFeatureFromLocation(location);

    for(int i = 0; i < newFeatures.length; i++) {
      kmeans.update(newFeatures[i],location);
    }

    double[][] centroids = kmeans.getCentroids();
    collector.emit(new Values(input.getValue(0), centroids));

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cloudID", "centroids"));
  }
}
