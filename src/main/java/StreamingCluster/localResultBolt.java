package StreamingCluster;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by ken on 11/17/2015.
 */
public class localResultBolt extends BaseBasicBolt {
  public localResultBolt(boolean ackEnabled) {
  }

  public void execute(Tuple input, BasicOutputCollector collector) {

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}
