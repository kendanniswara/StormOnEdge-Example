package KMeansStreaming;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by ken on 11/17/2015.
 */
public class ClusteringBoltLocal extends BaseBasicBolt {

  private FurthestPointKMeans kmeans;
  private OutputCollector _collector;
  private Map stormConf;

  public ClusteringBoltLocal(int clusterGroup, boolean ackEnabled) {

    kmeans = new FurthestPointKMeans(clusterGroup, 0.2);
    kmeans.reset();
  }

  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    stormConf = conf;
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    double[] feature = (double[]) input.getValue(1);

    boolean update = kmeans.update(feature, "");

    if(update) {
      double[][] centroids = kmeans.getCentroids();
      collector.emit(new Values(input.getValue(0), centroids));
    }

  }



  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cloudID", "centroids"));
  }
}
