package StreamingKMeans;

/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import NetworkEvaluation.*;
import StormOnEdge.grouping.stream.*;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class ClusteringTopology {

  private static final Log LOG = LogFactory.getLog(Main.class);

  @Option(name="--help", aliases={"-h"}, usage="print help message")
  private boolean _help = false;

  @Option(name="--debug", aliases={"-d"}, usage="enable debug")
  private boolean _debug = false;

  @Option(name="--localTaskGroup", aliases={"--localGroup"}, metaVar="LOCALGROUP",
          usage="number of initial local TaskGroup")
  private int _localGroup = 2;

  @Option(name="--spoutParallel", aliases={"--spout"}, metaVar="SPOUT",
          usage="number of spouts to run local TaskGroup")
  private int _spoutParallel = 2;

  @Option(name="--boltParallelLocal", aliases={"--boltLocal"}, metaVar="BOLTLOCAL",
          usage="number of bolts to run local TaskGroup")
  private int _boltLocalParallel = 2;

  @Option(name="--boltParallelGlobal", aliases={"--boltGlobal"}, metaVar="BOLTGLOBAL",
          usage="number of bolts to run global TaskGroup")
  private int _boltGlobalParallel = 2;

  @Option(name="--numWorkers", aliases={"--workers"}, metaVar="WORKERS",
      usage="number of workers to use per topology")
  private int _numWorkers = 50;

  @Option(name="--ackEnabled", aliases={"--ack"}, usage="enable acking")
  private boolean _ackEnabled = false;

  @Option(name="--ackers", metaVar="ACKERS",
      usage="number of acker bolts to launch per topology")
  private int _ackers = 3;

  @Option(name="--pollFreqSec", aliases={"--pollFreq"}, metaVar="POLL",
      usage="How often should metrics be collected")
  private int _pollFreqSec = 30;

  @Option(name="--testTimeSec", aliases={"--testTime"}, metaVar="TIME",
      usage="How long should the benchmark run for.")
  private int _testRunTimeSec = 5 * 60;

  @Option(name="--sampleRateSec", aliases={"--sampleRate"}, metaVar="SAMPLE",
	      usage="Sample rate for metrics (0-1).")
	  private double _sampleRate = 0.3;

  private String _name = "KMeansTest";

  private static class MetricsState {
    long transferred = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }

  private boolean printOnce = true;


  public void metrics(Client client, int poll, int total) throws Exception {
    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\tthroughput (MB/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(client, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime)/pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    do {

    	/// one time print addition
        if(printOnce)
        {
      	  printExecutorLocation(client);
        }
        printOnce = false;
        ///

      metrics(client, now, state, "RUNNING");

      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
  }

  private void printExecutorLocation(Client client) throws Exception {
	  ClusterSummary summary = client.getClusterInfo();
	  StringBuilder executorBuilder = new StringBuilder();

	  for (TopologySummary ts: summary.get_topologies()) {
	      String id = ts.get_id();
	      TopologyInfo info = client.getTopologyInfo(id);

	      executorBuilder.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
	      for (ExecutorSummary es: info.get_executors()) {
	    	  executorBuilder.append(es.get_executor_info().get_task_start() +"," + es.get_component_id() + "," + es.get_host() + "\n");
	      }
	      executorBuilder.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
	  }

	  System.out.println(executorBuilder.toString());
}

public boolean metrics(Client client, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    long time = now - state.lastTime;
    state.lastTime = now;
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;

    //////////
    //String namaSupervisor = "";
    for (SupervisorSummary sup: summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
      //namaSupervisor = namaSupervisor + sup.get_host() + ",";
    }
    //System.out.println(namaSupervisor);

    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts: summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);

      ////SOE Addition
      PerftestWriter.print(summary, info, new HashMap<String, Long>());
      ////

      for (ExecutorSummary es: info.get_executors()) {
        ExecutorStats stats = es.get_stats();
        totalExecutors++;
        if (stats != null) {
          Map<String,Map<String,Long>> transferred = stats.get_emitted();/* .get_transferred();*/
          if ( transferred != null) {
            Map<String, Long> e2 = transferred.get(":all-time");
            if (e2 != null) {
              executorsWithMetrics++;
              //The SOL messages are always on the default stream, so just count those
              Long dflt = e2.get("default");
              if (dflt != null) {
                totalTransferred += dflt;
              }
            }
          }
        }
      }
    }
    //long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    //double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : (transferredDiff * size)/(1024.0 * 1024.0)/(time/1000.0);
    //System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+totalExecutors+"\t"+executorsWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);
	System.out.println(message+","+totalSlots+","+totalUsedSlots+","+totalExecutors+","+executorsWithMetrics+","+time+",NOLIMIT");
    if ("WAITING".equals(message)) {
      //System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
    }
    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
  }


  public void realMain(String[] args) throws Exception {
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
    
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      _help = true;
    }
    if(_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }
    if (!_ackEnabled) {
      _ackers = 0;
    }

    try {

        int totalSpout = _spoutParallel * _localGroup;
        int totalLocalBolt = _boltLocalParallel * _localGroup;
        int totalLocalResultBolt = _localGroup;
        int totalGlobalBolt = _boltGlobalParallel;
        int totalGlobalResultBolt = 1;

        int _clusteringGroupSize = 4;
        double _clusteringErrorRate = 0.2;
        long _expireTime = 5000;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("KmeansSpout", new ClusteringSpout(_ackEnabled), totalSpout).addConfiguration("group-name", "local1");

        builder.setBolt("ClusteringBoltLocal", new ClusteringBoltLocal(_clusteringGroupSize, _clusteringErrorRate, _expireTime, _ackEnabled), totalLocalBolt)
                .customGrouping("KmeansSpout", new ZoneShuffleGrouping())
                .addConfiguration("group-name", "local1");

        builder.setBolt("resultBoltLocal", new ResultBolt(_ackEnabled), totalLocalResultBolt)
                .customGrouping("ClusteringBoltLocal", new ZoneShuffleGrouping())
                .addConfiguration("group-name", "local1");

        builder.setBolt("ClusteringBoltGlobal", new ClusteringBoltGlobal(_clusteringGroupSize,_clusteringErrorRate, -1, _ackEnabled), totalGlobalBolt)
                .customGrouping("ClusteringBoltLocal", new ZoneShuffleGrouping())
                .addConfiguration("group-name", "global1");

        builder.setBolt("resultBoltGlobal", new ResultBolt(_ackEnabled), totalGlobalResultBolt)
                .shuffleGrouping("ClusteringBoltGlobal")
                .addConfiguration("group-name", "global1");

        Config conf = new Config();
        conf.setDebug(_debug);
        conf.setNumWorkers(_numWorkers);
        conf.setNumAckers(_ackers);
        conf.setStatsSampleRate(_sampleRate);

        StormSubmitter.submitTopology(_name, conf, builder.createTopology());

      metrics(client, _pollFreqSec, _testRunTimeSec);

    } finally {
      //Kill it right now!!!
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(0);

        LOG.info("KILLING "+_name);
        try {
          client.killTopologyWithOpts(_name, killOpts);
        } catch (Exception e) {
          LOG.error("Error tying to kill "+_name,e);
        }
    }
  }
  
  public static void main(String[] args) throws Exception {
    new ClusteringTopology().realMain(args);
  }
}

