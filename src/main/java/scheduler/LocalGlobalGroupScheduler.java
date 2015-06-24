package scheduler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mortbay.util.MultiMap;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class LocalGlobalGroupScheduler implements IScheduler {
	
	Random rand = new Random(System.currentTimeMillis());
	JSONParser parser = new JSONParser();
	CloudLocator clocator = new CloudLocator("/home/kend/fromSICSCloud/Scheduler-MinMaxMatrix.txt");
	
    public void prepare(Map conf) {}

    @SuppressWarnings("unchecked")
	public void schedule(Topologies topologies, Cluster cluster) {
    
	    System.out.println("NetworkAwareGroupScheduler: begin scheduling");
	
	    HashMap<String,String[]> spoutCloudsPair = new HashMap<String, String[]>();
	    List<String> cloudNameList = new ArrayList<String>();
	    LinkedHashMap<String, SchedulerGroup> localGroupNameList = new LinkedHashMap<String,SchedulerGroup>();
	    LinkedHashMap<String, SchedulerGroup> globalGroupNameList = new LinkedHashMap<String,SchedulerGroup>();
	    
	    //Reading the information from file
	    FileReader pairDataFile; 
	    BufferedReader textReader;
	    String line;
	    try {
		    
	    	//---------------------------
		    //get spout & cloud_list pair
		    //---------------------------
		    pairDataFile = new FileReader("/home/kend/fromSICSCloud/Scheduler-SpoutCloudsPair.txt");
		    textReader  = new BufferedReader(pairDataFile);
		    
		    line = textReader.readLine();
		    while(line != null && !line.equals(""))
		    {
		    	//Format
		    	//SpoutID;cloudA,cloudB,cloudC
		    	System.out.println("Read from file: " + line);
		    	String[] pairString = line.split(";");
		    	String[] cloudList = pairString[1].split(",");
		    	spoutCloudsPair.put(pairString[0],cloudList);
		    	
		    	line = textReader.readLine();
		    }
		    
		    textReader.close();
		    
		    //---------------
		    //get cloud names
		    //---------------
		    pairDataFile = new FileReader("/home/kend/fromSICSCloud/Scheduler-CloudList.txt");
		    textReader  = new BufferedReader(pairDataFile);
		    
		    line = textReader.readLine();
		    while(line != null && !line.equals(""))
		    {
		    	//Format
		    	//cloudName
		    	System.out.println("Read from file: " + line);
		    	cloudNameList.add(line);
		    	
		    	line = textReader.readLine();
		    }
		    textReader.close();
		    
		    
		    //---------------
		    //get group names
		    //---------------
		    pairDataFile = new FileReader("/home/kend/fromSICSCloud/Scheduler-GroupList.txt");
		    textReader  = new BufferedReader(pairDataFile);
		    
		    line = textReader.readLine();
		    while(line != null && !line.equals(""))
		    {
		    	//Format
		    	//Global1 / Local1
		    	System.out.println("Read from file: " + line);
		    	if(line.contains("Local"))
		    		localGroupNameList.put(line,new SchedulerGroup(line));		    	
		    	else if(line.contains("Global"))
		    		globalGroupNameList.put(line,new SchedulerGroup(line));
		    	
		    	line = textReader.readLine();
		    }
		    textReader.close();
		    
		    
	    }catch(IOException e){
	    	System.out.println("Some exception happened");
	    	System.out.println(e.getMessage());
	    	e.printStackTrace();
	    	}
    
	    
	    System.out.println("Start categorizing the supervisor");
	    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
	    
	    MultiMap supervisorsByCloudName = new MultiMap();
        MultiMap workersByCloudName = new MultiMap();
        MultiMap tasksByCloudName = new MultiMap();
	    
        //map the supervisors based on cloud names
        for (SupervisorDetails supervisor : supervisors) {
        	Map<String, Object> metadata = (Map<String, Object>)supervisor.getSchedulerMeta();
        	if(metadata.get("cloud-name") != null){
        		supervisorsByCloudName.add(metadata.get("cloud-name"), supervisor);
        		workersByCloudName.addValues(metadata.get("cloud-name"), cluster.getAvailableSlots(supervisor));
        	}
        }
        
        //print the worker list
        for(Object cloudNameKey : supervisorsByCloudName.keySet())
        {
        	String key = (String)cloudNameKey;
        	System.out.println(key + " :");
        	//System.out.println("Supervisors: " + supervisorsByCloudName.getValues(key));
        	System.out.println("Workers: " + workersByCloudName.getValues(key));
        	System.out.println("");
        }
        
        if(spoutCloudsPair.size() == 0 || cloudNameList.size() == 0 || globalGroupNameList.size() == 0)
        {
        	System.out.println("Reading is not complete, stop scheduling for now");
        	return;
        }
        
		for (TopologyDetails topology : topologies.getTopologies()) {
			
			if (!cluster.needsScheduling(topology) || cluster.getNeedsSchedulingComponentToExecutors(topology).isEmpty()) {
	            		System.out.println("This topology doesn't need scheduling.");
			}
			else {
				
				MultiMap executorWorkerMap = new MultiMap();
				MultiMap executorCloudMap = new MultiMap();
				
				StormTopology st = topology.getTopology();
				Map<String, Bolt> bolts = st.get_bolts();
				Map<String, SpoutSpec> spouts = st.get_spouts();
				
				Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
				System.out.println("needs scheduling(component->executor): " + componentToExecutors);

				System.out.println("LOG: Categorizing Spouts into schedulergroup");
				for(String name : spouts.keySet()){
                    SpoutSpec s = spouts.get(name);
                    
					try {
						JSONObject conf = (JSONObject)parser.parse(s.get_common().get_json_conf());
						
						if(conf.get("group-name") != null){
							String groupName = (String)conf.get("group-name");
							SchedulerGroup schedulergroup;
							
							//each task only reside in one group
							if(localGroupNameList.containsKey(groupName))
								schedulergroup = localGroupNameList.get(groupName);
							else
								schedulergroup = globalGroupNameList.get(groupName);
							
							if(schedulergroup != null)
							{
								schedulergroup.spoutNames.add(name);
								schedulergroup.spoutParallel.put(name, s.get_common().get_parallelism_hint());
								for(String cloudName : spoutCloudsPair.get(name))
								{
									schedulergroup.clouds.add(cloudName);
								}
							}
						}
						
					}catch(ParseException e){e.printStackTrace();}
				}
				
				System.out.println("LOG: Categorizing Bolts into schedulergroup");
				for(String name : bolts.keySet()){
					System.out.println(name);
					
                    Bolt b = bolts.get(name);
                    Set<GlobalStreamId> inputStreams = b.get_common().get_inputs().keySet();
                    
					try {
						JSONObject conf = (JSONObject)parser.parse(b.get_common().get_json_conf());
						
						if(conf.get("group-name") != null){
							String groupName = (String)conf.get("group-name");
							SchedulerGroup schedulergroup;
							
							//each task only reside in one group
							if(localGroupNameList.containsKey(groupName))
								schedulergroup = localGroupNameList.get(groupName);
							else
								schedulergroup = globalGroupNameList.get(groupName);
							
							if(schedulergroup != null)
							{
								schedulergroup.boltNames.add(name);
								schedulergroup.boltParallel.put(name, b.get_common().get_parallelism_hint());
								
								for(GlobalStreamId streamId : inputStreams)
								{
									System.out.println("--dependent to " + streamId.get_componentId());
									schedulergroup.boltDependence.add(streamId.get_componentId());
								}
							}
						}
						
					}catch(ParseException e){e.printStackTrace();}
				}
				

				//Local group task distribution
				//for each spouts:
				//get clouds 
				for(SchedulerGroup localGroup : localGroupNameList.values())
				{
					try {
					
						System.out.println("LOG: " + localGroup.name + "distribution");
						
						for(String spout : localGroup.spoutNames)
						{
							System.out.println("-" + spout);
							List<ExecutorDetails> executors = componentToExecutors.get(spout);
							int parHint = localGroup.spoutParallel.get(spout);
							
							if(executors == null)
			            	{
			            		System.out.println(localGroup.name + ": " + spout + ": No executors");
			            	}
			            	else
			            	{
			            		int cloudIndex = 0;
			            		int exPerCloud = parHint / localGroup.clouds.size(); //for now, only work on even number between executors to workers
			            		for(String cloudName : localGroup.clouds)
			            		{
			            			int startidx = cloudIndex * exPerCloud;
			            			int endidx = startidx + exPerCloud;
			            			
			            			if (endidx > executors.size())
			            				endidx = executors.size() - 1;
			            			
			            			List<ExecutorDetails> subexecutors = executors.subList(startidx, endidx);
			            			List<WorkerSlot> workers = (List<WorkerSlot>) workersByCloudName.get(cloudName);
			            			
			            			System.out.println("---" + cloudName + "\n" + "-----subexecutors:" + subexecutors);
			            			System.out.println("-----workers:" + workers);
			            			
			            			if(workers == null || workers.isEmpty())
			    	            		System.out.println(localGroup.name + ": " + cloudName + ": No workers");
			            			else
			            			{
			            				deployExecutorToWorkers(workers, subexecutors, executorWorkerMap);
			            				executorCloudMap.add(spout, cloudName);
			            				
			            				for(ExecutorDetails ex : subexecutors)
		            	        			tasksByCloudName.add(cloudName, ex.getStartTask());
			            			}
			            			
			            			cloudIndex++;
			            		}
			            	}
						}
						
						for(String bolt : localGroup.boltNames)
						{
							System.out.println("-" + bolt);
							List<ExecutorDetails> executors = componentToExecutors.get(bolt);
							int parHint = localGroup.boltParallel.get(bolt);
							
							if(executors == null)
			            	{
			            		System.out.println(localGroup.name + ": " + bolt + ": No executors");
			            	}
			            	else
			            	{
			            		int cloudIndex = 0;
			            		int exPerCloud = parHint / localGroup.clouds.size(); //to be safe, only work on even number between executors to workers
			            		for(String cloudName : localGroup.clouds)
			            		{
			            			int startidx = cloudIndex * exPerCloud;
			            			int endidx = startidx + exPerCloud;
			            			
			            			if (endidx > executors.size())
			            				endidx = executors.size() - 1;
			            			
			            			List<ExecutorDetails> subexecutors = executors.subList(startidx, endidx);
			            			List<WorkerSlot> workers = (List<WorkerSlot>) workersByCloudName.get(cloudName);
			            			
			            			System.out.println("---" + cloudName + "\n" + "-----subexecutors:" + subexecutors);
			            			System.out.println("-----workers:" + workers);
			            			
			            			if(workers == null || workers.isEmpty())
			    	            		System.out.println(localGroup.name + ": " + cloudName + ": No workers");
			            			else
			            			{
			            				deployExecutorToWorkers(workers, subexecutors, executorWorkerMap);
			            				executorCloudMap.add(bolt, cloudName);
			            				
			            				for(ExecutorDetails ex : subexecutors)
			            	        			tasksByCloudName.add(cloudName, ex.getStartTask());			            				
			            			}
			            			
			            			cloudIndex++;
			            		}
			            	}
						}
					} catch(Exception e) {
						System.out.println(e);
						}
				}
				
				
				//Global group task distribution
				for(SchedulerGroup globalGroup : globalGroupNameList.values())
				{
					try {
						System.out.println("LOG: " + globalGroup.name + " distribution");
						
						Set<String> cloudDependencies = new HashSet<String>();
						for(String dependentExecutors : globalGroup.boltDependence)
						{
							if(executorCloudMap.getValues(dependentExecutors) == null)
								continue;
							else
								cloudDependencies.addAll((List<String>) executorCloudMap.getValues(dependentExecutors));
						}
						
						System.out.println("-cloudDependencies: " + cloudDependencies);
						
						String choosenCloud = clocator.MinMaxLatency(cloudNameList, cloudDependencies);
						
						System.out.println("-choosenCloud: " + choosenCloud);
						
						for(String bolt : globalGroup.boltNames)
						{
							System.out.println("---" + bolt);
							List<ExecutorDetails> executors = componentToExecutors.get(bolt);
							List<WorkerSlot> workers = (List<WorkerSlot>) workersByCloudName.get(choosenCloud);
							
							System.out.println("-----subexecutors:" + executors);
	            			System.out.println("-----workers:" + workers);
							
							if(executors == null)
			            		System.out.println(globalGroup.name + ": " + bolt + ": No executors");
			            	else if(workers.isEmpty())
	    	            		System.out.println(globalGroup.name + ": " + choosenCloud + ": No workers");
		            		else {
	            				deployExecutorToWorkers(workers, executors, executorWorkerMap);
	            				executorCloudMap.add(bolt, choosenCloud);
	            				
	            				for(ExecutorDetails ex : executors)
	            	        			tasksByCloudName.add(choosenCloud, ex.getStartTask());	            				
		            		}
						}
					
					} catch(Exception e) {
						System.out.println(e);
						}
				}

	            //Assign the tasks into cluster
	            StringBuilder workerStringBuilder = new StringBuilder();
	            for(Object ws : executorWorkerMap.keySet())
	        	{
	            	List<ExecutorDetails> edetails = (List<ExecutorDetails>) executorWorkerMap.getValues(ws);
	            	WorkerSlot wslot = (WorkerSlot) ws;
	            	
	        		cluster.assign(wslot, topology.getId(), edetails);
	        		System.out.println("We assigned executors:" + executorWorkerMap.getValues(ws) + " to slot: [" + wslot.getNodeId() + ", " + wslot.getPort() + "]");
	        		workerStringBuilder.append(executorWorkerMap.getValues(ws) + " to slot: [" + wslot.getNodeId() + ", " + wslot.getPort() + "]\n");
	        	}
	            
	            try {
					FileWriter writer = new FileWriter("/home/kend/SchedulerResult.csv", true);
					writer.write(workerStringBuilder.toString());
					writer.close();
				}catch(Exception e){ }
	            
	            printTaskCloudPairs(tasksByCloudName);
	        }
	    }
	        
	        // let system's even scheduler handle the rest scheduling work
	        // you can also use your own other scheduler here, this is what
	        // makes storm's scheduler composable.
	        new EvenScheduler().schedule(topologies, cluster);
    }

	private void deployExecutorToWorkers(List<WorkerSlot> workers, List<ExecutorDetails> executors, MultiMap executorWorkerMap)
    {
    	Iterator<WorkerSlot> workerIterator = workers.iterator();
    	Iterator<ExecutorDetails> executorIterator = executors.iterator();

    	//if more executors than workers, do simple round robin
    	//for all executors A to all supervisors B
    	if(executors.size() >= workers.size())
    	{
        	while(executorIterator.hasNext() && workerIterator.hasNext())
        	{
        		WorkerSlot w = workerIterator.next();
        		ExecutorDetails ed = executorIterator.next();
        		executorWorkerMap.add(w, ed);
        		
        		//String SupName = findSupervisorNameinWorkerSlot(workerSlotClusterBySupervisorMap, w);
        		//for(int ii = ed.getStartTask(); ii <= ed.getEndTask(); ii++)
        			//taskClusterBySupervisorMap.add(SupName, new Integer(ii));
        		
        		//reset to 0 again
        		if(!workerIterator.hasNext())
        			workerIterator = workers.iterator();
        	}
    	}
    	
    	//if more workers than executors, choose randomly
    	//for all executors A to all supervisors B
    	else
    	{
        	while(executorIterator.hasNext() && !workers.isEmpty())
        	{
        		WorkerSlot w = workers.get(rand.nextInt(workers.size()));
        		ExecutorDetails ed = executorIterator.next();
        		executorWorkerMap.add(w, ed);
        		
        		//String SupName = findSupervisorNameinWorkerSlot(workerSlotClusterBySupervisorMap, w);
        		//for(int ii = ed.getStartTask(); ii <= ed.getEndTask(); ii++)
        			//taskClusterBySupervisorMap.add(SupName, new Integer(ii));
        	}
    	}
    }

	//create a file pair of CloudName and tasks assigned to this cloud
	//This file is needed for intra-cloud grouping
	@SuppressWarnings("unchecked")
	private void printTaskCloudPairs(MultiMap tasksByCloudName)
	{
		System.out.println("tasksByCloudName: " + tasksByCloudName.size());
		
		StringBuilder taskStringBuilder = new StringBuilder();            
		for(Object sup : tasksByCloudName.keySet())
		{
			String taskString = (String) sup + ";";
		
			for(Integer t : (List<Integer>) tasksByCloudName.getValues(sup))
			{
				taskString = taskString + t.toString() + ",";
			}
		
			taskStringBuilder.append(taskString.substring(0, taskString.length()-1));
			taskStringBuilder.append("\n");
			
			System.out.println(taskStringBuilder.toString());
		}
         
        try 
        {
			FileWriter writer = new FileWriter("/home/kend/fromSICSCloud/PairSupervisorTasks.txt", true);
			writer.write(taskStringBuilder.toString());
			writer.close();
		} catch(Exception e){ }
	}
}

class SchedulerGroup {
	public String name;
	public List<String> clouds = new ArrayList<String>();
	public List<String> spoutNames = new ArrayList<String>();
	public Map<String,Integer> spoutParallel = new HashMap<String, Integer>();
	public List<String> boltNames = new ArrayList<String>();
	public Map<String,Integer> boltParallel = new HashMap<String, Integer>();
	public Set<String> boltDependence = new HashSet<String>();
	
	public SchedulerGroup(String Groupname) {
		name = Groupname;
	}
}