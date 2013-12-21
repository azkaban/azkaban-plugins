/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import azkaban.jobtype.pigutils.PigJobStats;

import org.apache.pig.tools.pigstats.JobStats;

public class JobDagNode {
	private String name;
	private String jobId;
	private List<String> aliases;
	private List<String> features;

	private List<String> parents = new ArrayList<String>();
	private List<String> successors = new ArrayList<String>();
  private PigJobStats jobStats;
	
	private MapReduceJobState mapReduceJobState;
	private Properties jobConfiguration;
	private int level = 0;

	public JobDagNode() {
	}

	public JobDagNode(String name, String[] aliases, String[] features) {
		this.name = name;
		this.aliases = Arrays.asList(aliases);
		this.features = Arrays.asList(features);
	}

	public JobDagNode(String name, List<String> aliases, List<String> features) {
		this.name = name;
		this.aliases = aliases;
		this.features = features;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public List<String> getAliases() {
		return aliases;
	}

	public List<String> getFeatures() {
		return features;
	}

	public void addParent(JobDagNode parent) {
		parents.add(parent.getJobId());
	}

	public void setParents(List<String> parents) {
		this.parents = parents;
	}

	public List<String> getParents() {
		return parents;
	}

	public void addSuccessor(JobDagNode successor) {
		successors.add(successor.getJobId());
	}

	public void setSuccessors(List<String> successors) {
		this.successors = successors;
	}

	public List<String> getSuccessors() {
		return successors;
	}

	public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
		this.mapReduceJobState = mapReduceJobState;
	}

	public MapReduceJobState getMapReduceJobState() {
		return mapReduceJobState;
	}

	public void setJobConfiguration(Properties jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
	}

	public void setJobStats(PigJobStats pigJobStats) {
		this.jobStats = pigJobStats;
	}

  public void setJobStats(JobStats jobStats) {
    this.jobStats = new PigJobStats(jobStats);
  }

	public PigJobStats getJobStats() {
		return jobStats;
	}

	// XXX Refactor this!
	private static Object propertiesToJson(Properties properties) {
		Map<String, String> jsonObj = new HashMap<String, String>();
		Set<String> keys = properties.stringPropertyNames();
		for (String key : keys) {
			jsonObj.put(key, properties.getProperty(key));
		}
		return jsonObj;
	}

	private static Properties propertiesFromJson(Object obj) {
		Map<String, String> jsonObj = (HashMap<String, String>) obj;
		Properties properties = new Properties();
		for (Map.Entry<String, String> entry : jsonObj.entrySet()) {
			properties.setProperty(entry.getKey(), entry.getValue());
		}
		return properties;
	}

	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("name", name);
		jsonObj.put("jobId", jobId);
		jsonObj.put("level", Integer.toString(level));
		jsonObj.put("aliases", Arrays.asList(aliases));
		jsonObj.put("features", Arrays.asList(features));
		jsonObj.put("parents", parents);
		jsonObj.put("successors", successors);
		if (jobConfiguration != null) {
			jsonObj.put("jobConfiguration", propertiesToJson(jobConfiguration));
		}
		jsonObj.put("jobStats", jobStats.toJson());
		jsonObj.put("mapReduceJobState", mapReduceJobState.toJson());
		return jsonObj;
	}

	@SuppressWarnings("unchecked")
	public static JobDagNode fromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		String name = (String) jsonObj.get("name");
		List<String> aliases = (ArrayList<String>) jsonObj.get("aliases");
		List<String> features = (ArrayList<String>) jsonObj.get("features");

		JobDagNode node = new JobDagNode(name, aliases, features);
		node.setJobId((String) jsonObj.get("jobId"));
		node.setParents((ArrayList<String>) jsonObj.get("parents"));
		node.setSuccessors((ArrayList<String>) jsonObj.get("successors"));

    // Grab configuration if it is available.
		if (jsonObj.containsKey("jobConfiguration")) {
			node.setJobConfiguration(
					propertiesFromJson(jsonObj.get("jobConfiguration")));
		}
		node.setLevel(Integer.parseInt((String) jsonObj.get("level")));

    // Grab PigJobStats;
    PigJobStats pigJobStats = PigJobStats.fromJson(jsonObj.get("jobStats"));
    node.setJobStats(pigJobStats);
		
		// Grab MapReduceJobState.
		MapReduceJobState mapReduceJobState = 
				MapReduceJobState.fromJson(jsonObj.get("mapReduceJobState"));
		node.setMapReduceJobState(mapReduceJobState);

		return node;
	}
}
