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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;
import com.twitter.ambrose.pig.PigJob;
import com.twitter.ambrose.util.JSONUtil;

import azkaban.utils.Props;

public class AzkabanPigListener implements PigProgressNotificationListener{

	private static Logger logger = Logger.getLogger(AzkabanPigListener.class);
	private String outputDir = ".";
	private String outputDagNodeFile;
	private String outputDagNodeJobIdFile;
	private String outputCompletedJobIdsFile;
	
	private List<Job> jobs = new ArrayList<Job>();
	private Map<String, DAGNode<PigJob>> dagNodeNameMap = 
			new HashMap<String, DAGNode<PigJob>>();
	private Map<String, DAGNode<PigJob>> dagNodeJobIdMap = 
			new HashMap<String, DAGNode<PigJob>>();
	private Set<String> completedJobIds = new HashSet<String>();
	
	public AzkabanPigListener(Props props) {
		outputDir = props.getString("pig.listener.output.dir", ".");
		String jobId = props.getString("azkaban.job.id");
		outputDagNodeFile = outputDir + "/" + jobId + "-dagnodemap.json";
		outputDagNodeJobIdFile = outputDir + "/" + jobId + "-dagnodejobidmap.json";
		outputCompletedJobIdsFile = outputDir + "/" + jobId + "-completedjobs.json";
	}
	
	@Override
	public void initialPlanNotification(String scriptId, MROperPlan plan) {
		logger.info("**********initialPlanNotification!**********");
		
		//logger.info("The script id is " + scriptId);
		//logger.info("The plan is " + plan.toString());
		
		// borrowed from ambrose
		Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();

		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
			String nodeName = entry.getKey().toString();
			String[] aliases = toArray(
					ScriptState.get().getAlias(entry.getValue()).trim());
			String[] features = toArray(
					ScriptState.get().getPigFeature(entry.getValue()).trim());

			DAGNode<PigJob> node = new DAGNode<PigJob>(
					nodeName, new PigJob(aliases, features));

			this.dagNodeNameMap.put(node.getName(), node);

			// This shows how we can get the basic info about all nameless jobs 
			// before any execute. We can traverse the plan to build a DAG of this 
			// info.
			logger.info("initialPlanNotification: aliases: " + 
					StringUtils.join(aliases, ",") + ", name: " + node.getName() + 
					", features: " + StringUtils.join(features, ","));
		}

		// second pass connects the edges
		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
			DAGNode<PigJob> node = this.dagNodeNameMap.get(entry.getKey().toString());
			List<DAGNode<? extends Job>> successorNodeList = 
					new ArrayList<DAGNode<? extends Job>>();
			List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());

			if (successors != null) {
				for (MapReduceOper successor : successors) {
					DAGNode<? extends Job> successorNode =
							this.dagNodeNameMap.get(successor.getOperatorKey().toString());
					successorNodeList.add(successorNode);
				}
			}

			node.setSuccessors(successorNodeList);
		}

		updateJsonFile();
	}

	private void updateJsonFile() {
		String dagNodeNameMapJson = null;
		String dagNodeJobIdMapJson = null;
		String completedJobIdsJson = null;
		try {
			dagNodeNameMapJson = JSONUtil.toJson(dagNodeNameMap.entrySet());
			dagNodeJobIdMapJson = JSONUtil.toJson(dagNodeJobIdMap.entrySet());
			completedJobIdsJson = JSONUtil.toJson(completedJobIds);
		}
		catch (Exception e) {
			logger.error("Failed to convert to json.");
		}
		try {
			JSONUtil.writeJson(outputDagNodeFile, dagNodeNameMapJson);
			JSONUtil.writeJson(outputDagNodeJobIdFile, dagNodeJobIdMapJson);
			JSONUtil.writeJson(outputCompletedJobIdsFile, completedJobIdsJson);
		}
		catch (IOException e) {
			logger.error("Couldn't write json file", e);
		}
	}

	@Override
	public void jobFailedNotification(String scriptId, JobStats stats) {
		if (stats.getJobId() == null) {
			logger.warn("jobId for failed job not found. This should only happen " +
					"in local mode");
			return;
		}

		DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
		if (node == null) {
			logger.warn("Unrecognized jobId reported for failed job: " + 
					stats.getJobId());
			return;
		}

		addCompletedJobStats(node.getJob(), stats);
		updateJsonFile();
	}

	@Override
	public void jobFinishedNotification(String scriptId, JobStats stats) {
		DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
		if (node == null) {
			logger.warn("Unrecognized jobId reported for succeeded job: " + 
					stats.getJobId());
			return;
		}
		addCompletedJobStats(node.getJob(), stats);
		updateJsonFile();
	}

	@Override
	public void jobStartedNotification(String scriptId, String assignedJobId) {
		logger.info("**********jobStartedNotification**********");
		PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
		logger.info("jobStartedNotification - jobId " + assignedJobId + 
				", jobGraph:\n" + jobGraph);

		// For each job in the graph, check if the stats for a job with this name 
		// is found. If so, look up it's scope and bind the jobId to the DAGNode 
		// with the same scope.
		for (JobStats jobStats : jobGraph) {
			if (assignedJobId.equals(jobStats.getJobId())) {
				logger.info("jobStartedNotification - scope " + jobStats.getName() + 
						" is jobId " + assignedJobId);
				DAGNode<PigJob> node = this.dagNodeNameMap.get(jobStats.getName());
				
				if (node == null) {
					logger.warn("jobStartedNotification - unrecognized operator name " +
							"found (" + jobStats.getName() + ") for jobId " + assignedJobId);
				} else {
					node.getJob().setId(assignedJobId);
					addMapReduceJobState(node.getJob());
					dagNodeJobIdMap.put(node.getJob().getId(), node);
					updateJsonFile();
				}
			}
		}
	}

	@Override
	public void jobsSubmittedNotification(String arg0, int arg1) {
		logger.info("jobSubmittedNotification");
		logger.info("The script id is " + arg0);
		logger.info(arg1 + " jobs submitted.");
	}

	@Override
	public void launchCompletedNotification(String arg0, int arg1) {
		logger.info("launchCompletedNotification");
		logger.info("The script id is " + arg0);
		logger.info("Finished " + arg1 + " jobs successfully"); 
	}

	@Override
	public void launchStartedNotification(String arg0, int arg1) {
		logger.info("lanchStartedNotification");
		logger.info("launching script " + arg0);
		logger.info("launching " + arg1 + " mr jobs");
	}

	@Override
	public void outputCompletedNotification(String arg0, OutputStats arg1) {
		logger.info("outputCompletedNotification");
		logger.info("The script id is " + arg0);
		logger.info("The output stat name is " + arg1.getName());
		logger.info("You can get a lot more useful information here.");		
	}

	@Override
	public void progressUpdatedNotification(String scriptId, int progress) {
		// Then for each running job, we report the job progress.
		for (DAGNode<PigJob> node : dagNodeNameMap.values()) {
			// Don't send progress events for unstarted jobs.
			if (node.getJob().getId() == null) { continue; }
			addMapReduceJobState(node.getJob());

			// Only push job progress events for a completed job once.
			if (node.getJob().getMapReduceJobState() != null && 
					!completedJobIds.contains(node.getJob().getId())) {
				if (node.getJob().getMapReduceJobState().isComplete()) {
					completedJobIds.add(node.getJob().getId());
				}
			}
		}
		updateJsonFile();
	}
	
	private static String[] toArray(String string) {
		return string == null ? new String[0] : string.trim().split(",");
	}

	@SuppressWarnings("deprecation")
	private void addMapReduceJobState(PigJob pigJob) {
		JobClient jobClient = PigStats.get().getJobClient();
		
		try {
			RunningJob runningJob = jobClient.getJob(pigJob.getId());
			if (runningJob == null) {
				logger.warn("Couldn't find job status for jobId=" + pigJob.getId());
				return;
			}

			JobID jobID = runningJob.getID();
			TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
			TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
			pigJob.setMapReduceJobState(
					new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport));

			Properties jobConfProperties = new Properties();
			Configuration conf = jobClient.getConf();
			for (Map.Entry<String, String> entry : conf) {
				jobConfProperties.setProperty(entry.getKey(), entry.getValue());
			}
			pigJob.setConfiguration(jobConfProperties);

		} catch (IOException e) {
			logger.error("Error getting job info.", e);
		}
	}
	
	private void addCompletedJobStats(PigJob job, JobStats stats) {
		// Put the job conf into a Properties object so we can serialize them.
		Properties jobConfProperties = new Properties();
		if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
				stats.getInputs().get(0).getConf() != null) {
			Configuration conf = stats.getInputs().get(0).getConf();
			for (Map.Entry<String, String> entry : conf) {
				jobConfProperties.setProperty(entry.getKey(), entry.getValue());
			}
		}
		job.setJobStats(stats);
		job.setConfiguration(jobConfProperties);
		jobs.add(job);
	}
	
}

