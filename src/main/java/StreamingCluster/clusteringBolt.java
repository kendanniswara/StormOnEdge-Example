package StreamingCluster;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by ken on 11/17/2015.
 */
public class clusteringBolt extends BaseBasicBolt {

  private FurthestPointKMeans kmeans;

  public clusteringBolt(int clusterGroup, boolean ackEnabled) {

    kmeans = new FurthestPointKMeans(clusterGroup, 0.2);
    kmeans.reset();
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    double[] feature = (double[]) input.getValue(1);

    boolean update = kmeans.update(feature);

    if(update) {
      double[][] centroids = kmeans.getCentroids();

      for(int i = 0; i < centroids.length; i++) {
        collector.emit(new Values(input.getValue(0), centroids[i]));
      }
    }

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}
