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

package azkaban.viewer.pigvisualizer;

public class JobDagNode {
	private String jobId;
	private String[] aliases;
	private String[] features;

	private Set<String> parents = new HashSet<String>();
	private Set<String> successors = new HashSet<String>();

	public JobDagNode() {
	}

	public JobDagNode(String jobId, String[] aliases, String[] features) {
		this.jobId = jobId;
		this.aliases = aliases;
		this.features = features;
	}

	public String getJobId() {
		return jobId;
	}

	public String[] getAliases() {
		return aliases;
	}

	public String[] getFeatures() {
		return features;
	}

	public void addParent(JobDagNode parent) {
		parents.put(parent);
	}

	public void addSuccessor(JobDagNode successor) {
		successors.put(successor);
	}
}
