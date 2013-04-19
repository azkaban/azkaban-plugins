package azkaban.jobtype;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
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
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.ScriptState;

import azkaban.utils.JSONUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * PigProgressNotificationListener that collects plan and job information from within a Pig runtime,
 * builds Ambrose model objects, and passes the objects to an Ambrose StatsWriteService object. This
 * listener can be used regardless of what mode Ambrose is running in.
 *
 * @see EmbeddedAmbrosePigProgressNotificationListener for a sublclass that can be used to run an
 * embedded Abrose web server from Pig client process.
 *
 */

public class AmbrosePigProgressNotificationListener implements PigProgressNotificationListener {
	protected Logger logger = Logger.getLogger(AmbrosePigProgressNotificationListener.class);

	private static final String RUNTIME = "pig";

	private String workflowVersion;
	private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();
//	private Map<String, DAGNode> dagNodeNameMap = new TreeMap<String, DAGNode>();

	private HashSet<String> completedJobIds = new HashSet<String>();
	
	private File metaDataFile = null;
	
	private Map<String, Object> metaData = new HashMap<String, Object>();
	
	public AmbrosePigProgressNotificationListener(File metaDataFile) {
		this.metaDataFile = metaDataFile;
	}
	
	public AmbrosePigProgressNotificationListener() {
	}

	protected static enum WorkflowProgressField {
		workflowProgress;
	}

	protected static enum JobProgressField {
		jobId, jobName, trackingUrl, isComplete, isSuccessful,
		mapProgress, reduceProgress, totalMappers, totalReducers;
	}

	private void updateMetaData(String key, Object value) {
		
		metaData.put(key, value);
		
		// update metaDataFile if any
		if(metaDataFile != null & metaDataFile.exists()) {
			try {
//				String jsonString = JSONUtils.toJSON(value);
				String jsonString = value.toString();
				FileWriter fw = new FileWriter(metaDataFile, true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(jsonString);
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * Intialize this class with an instance of StatsWriteService to push stats to.
	 *
	 * @param statsWriteService
	 */
	
//	public AmbrosePigProgressNotificationListener(StatsWriteService statsWriteService) {
//		this.statsWriteService = statsWriteService;
//	}
//
//	protected StatsWriteService getStatsWriteService() { return statsWriteService; }

	
	
	/**
	 * Called after the job DAG has been created, but before any jobs are fired.
	 * @param plan the MROperPlan that represents the DAG of operations. Each operation will become
	 * a MapReduce job when it's launched.
	 */
	@Override
	public void initialPlanNotification(String scriptId, MROperPlan plan) {
		Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();
		
		updateMetaData("initialPlanNotification", JSONUtils.toJSON(plan.toString()));
		
		System.out.println(scriptId);
		System.out.println(plan);
		
		// first pass builds all nodes
//		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
//			DAGNode node = new DAGNode(entry.getKey().toString(),
//					toArray(ScriptState.get().getAlias(entry.getValue())),
//					toArray(ScriptState.get().getPigFeature(entry.getValue())), RUNTIME);
//
//			this.dagNodeNameMap.put(node.getName(), node);
//
//			// this shows how we can get the basic info about all nameless jobs before any execute.
//			// we can traverse the plan to build a DAG of this info
//			logger.info("initialPlanNotification: alias: " + toString(node.getAliases())
//					+ ", name: " + node.getName() + ", feature: " + toString(node.getFeatures()));
//		}
//
//		// second pass connects the edges
//		for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
//			DAGNode node = this.dagNodeNameMap.get(entry.getKey().toString());
//			List<DAGNode> successorNodeList = new ArrayList<DAGNode>();
//			List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());
//
//			if (successors != null) {
//				for (MapReduceOper successor : successors) {
//					DAGNode successorNode = this.dagNodeNameMap.get(successor.getOperatorKey().toString());
//					successorNodeList.add(successorNode);
//				}
//			}
//
//			node.setSuccessors(successorNodeList);
//		}
//
//		try {
//			statsWriteService.sendDagNodeNameMap(null, this.dagNodeNameMap);
//		} catch (IOException e) {
//			logger.error("Couldn't send dag to StatsWriteService", e);
//		}
	}

	/**
	 * Called with a job is started. This is the first time that we are notified of a new jobId for a
	 * launched job. Hence this method binds the jobId to the DAGNode and pushes a status event.
	 * @param scriptId scriptId of the running script
	 * @param assignedJobId the jobId assigned to the job started.
	 */
	@Override
	public void jobStartedNotification(String scriptId, String assignedJobId) {
		PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
		logger.info("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);
		
		updateMetaData("jobStartedNotification", JSONUtils.toJSON(assignedJobId));

		System.out.println(scriptId);
		System.out.println(assignedJobId);
		System.out.println(jobGraph);
		
//		// for each job in the graph, check if the stats for a job with this name is found. If so, look
//		// up it's scope and bind the jobId to the DAGNode with the same scope.
//		for (JobStats jobStats : jobGraph) {
//			if (assignedJobId.equals(jobStats.getJobId())) {
//				logger.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
//				DAGNode node = this.dagNodeNameMap.get(jobStats.getName());
//
//				if (node == null) {
//					logger.warn("jobStartedNotification - unrecorgnized operator name found ("
//							+ jobStats.getName() + ") for jobId " + assignedJobId);
//				} else {
//					node.setJobId(assignedJobId);
//					pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_STARTED, node);
//
//					Map<JobProgressField, String> progressMap = buildJobStatusMap(assignedJobId);
//					if (progressMap != null) {
//						pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_PROGRESS, progressMap);
//					}
//				}
//			}
//		}
	}

	/**
	 * Called when a job fails. Mark the job as failed and push a status event.
	 * @param scriptId scriptId of the running script
	 * @param stats JobStats for the failed job.
	 */
	@Override
	public void jobFailedNotification(String scriptId, JobStats stats) {
//		JobInfo jobInfo = collectStats(scriptId, stats);
//		jobInfoList.add(jobInfo);
//		pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_FAILED, jobInfo);
		updateMetaData("jobFailedNotification", toJSONFromJobStats(stats));
	}

	/**
	 * Called when a job completes. Mark the job as finished and push a status event.
	 * @param scriptId scriptId of the running script
	 * @param stats JobStats for the completed job.
	 */
	@Override
	public void jobFinishedNotification(String scriptId, JobStats stats) {
		System.out.println(scriptId);
		System.out.println(stats);
		
		updateMetaData("jobFinishedNotification", toJSONFromJobStats(stats));
		
//		JobInfo jobInfo = collectStats(scriptId, stats);
//		jobInfoList.add(jobInfo);
//		pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_FINISHED, jobInfo);
	}

	/**
	 * Called after the launch of the script is complete. This means that zero or more jobs have
	 * succeeded and there is no more work to be done.
	 *
	 * @param scriptId scriptId of the running script
	 * @param numJobsSucceeded how many jobs have succeeded
	 */
	@Override
	public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
//
//		if (workflowVersion == null) {
//			logger.warn("scriptFingerprint not set for this script - not saving stats." );
//		} else {
//			WorkflowInfo workflowInfo = new WorkflowInfo(scriptId, workflowVersion, jobInfoList);
//
//			try {
//				outputStatsData(workflowInfo);
//			} catch (IOException e) {
//				log.error("Exception outputting workflowInfo", e);
//			}
//		}
	}

	/**
	 * Called throught execution of the script with progress notifications.
	 * @param scriptId scriptId of the running script
	 * @param progress is an integer between 0 and 100 the represents percent completion
	 */
	@Override
	public void progressUpdatedNotification(String scriptId, int progress) {
		System.out.println(scriptId);
		System.out.println(progress);
//
//		// first we report the scripts progress
//		Map<WorkflowProgressField, String> eventData = new HashMap<WorkflowProgressField, String>();
//		eventData.put(WorkflowProgressField.workflowProgress, Integer.toString(progress));
//		pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.WORKFLOW_PROGRESS, eventData);
//
//		// then for each running job, we report the job progress
//		for (DAGNode node : dagNodeNameMap.values()) {
//			// don't send progress events for unstarted jobs
//			if (node.getJobId() == null) { continue; }
//
//			Map<JobProgressField, String> progressMap = buildJobStatusMap(node.getJobId());
//
//			//only push job progress events for a completed job once
//			if (progressMap != null && !completedJobIds.contains(node.getJobId())) {
//				pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_PROGRESS, progressMap);
//
//				if ("true".equals(progressMap.get(JobProgressField.isComplete))) {
//					completedJobIds.add(node.getJobId());
//				}
//			}
//		}
	}

	/**
	 * Invoked just before launching MR jobs spawned by the script.
	 * @param scriptId the unique id of the script
	 * @param numJobsToLaunch the total number of MR jobs spawned by the script
	*/
	@Override
	public void launchStartedNotification(String scriptId, int numJobsToLaunch) { }

	/**
	 * Invoked just before launching MR jobs spawned by the script.
	 * @param scriptId the unique id of the script
	 * @param numJobsToLaunch the total number of MR jobs spawned by the script
	 */
	@Override
	public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) { }

	/**
	 * Invoked just after an output is successfully written.
	 * @param scriptId the unique id of the script
	 * @param outputStats the {@link OutputStats} object associated with the output
	*/
	@Override
	public void outputCompletedNotification(String scriptId, OutputStats outputStats) { 
		updateMetaData("outputCompletedNotification", toJSONFromPigStats(PigStats.get()));
	}

	/**
	* Collects statistics from JobStats and builds a nested Map of values. Subsclass ond override
	* if you'd like to generate different stats.
	*
	* @param scriptId
	* @param stats
	* @return
	*/
//	protected JobInfo collectStats(String scriptId, JobStats stats) {
//
//	// put the job conf into a Properties object so we can serialize them
//		Properties jobConfProperties = new Properties();
//		if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
//				stats.getInputs().get(0).getConf() != null) {
//
//			Configuration conf = stats.getInputs().get(0).getConf();
//			for (Map.Entry<String, String> entry : conf) {
//				jobConfProperties.setProperty(entry.getKey(), entry.getValue());
//			}
//
//			if (workflowVersion == null)  {
//				workflowVersion = conf.get("pig.logical.plan.signature");
//			}
//		}
//
//		return new PigJobInfo(stats, jobConfProperties);
//	}

//	private void outputStatsData(WorkflowInfo workflowInfo) throws IOException {
//		if(logger.isDebugEnabled()) {
//			logger.debug("Collected stats for script:\n" + WorkflowInfo.toJSON(workflowInfo));
//		}
//	}

//	private void pushEvent(String scriptId, WorkflowEvent.EVENT_TYPE eventType, Object eventData) {
//		try {
//			statsWriteService.pushEvent(scriptId, new WorkflowEvent(eventType, eventData, RUNTIME));
//		} catch (IOException e) {
//			logger.error("Couldn't send event to StatsWriteService", e);
//		}
//	}

	private Map<JobProgressField, String> buildJobStatusMap(String jobId)  {
		JobClient jobClient = PigStats.get().getJobClient();

		try {
			RunningJob rj = jobClient.getJob(jobId);
			if (rj == null) {
				logger.warn("Couldn't find job status for jobId=" + jobId);
				return null;
			}

			JobID jobID = rj.getID();
			TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
			TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
			Map<JobProgressField, String> progressMap = new HashMap<JobProgressField, String>();

			progressMap.put(JobProgressField.jobId, jobId.toString());
			progressMap.put(JobProgressField.jobName, rj.getJobName());
			progressMap.put(JobProgressField.trackingUrl, rj.getTrackingURL());
			progressMap.put(JobProgressField.isComplete, Boolean.toString(rj.isComplete()));
			progressMap.put(JobProgressField.isSuccessful, Boolean.toString(rj.isSuccessful()));
			progressMap.put(JobProgressField.mapProgress, Float.toString(rj.mapProgress()));
			progressMap.put(JobProgressField.reduceProgress, Float.toString(rj.reduceProgress()));
			progressMap.put(JobProgressField.totalMappers, Integer.toString(mapTaskReport.length));
			progressMap.put(JobProgressField.totalReducers, Integer.toString(reduceTaskReport.length));
			return progressMap;
		} catch (IOException e) {
			logger.error("Error getting job info.", e);
		}

		return null;
	}

	private static String[] toArray(String string) {
		return string == null ? new String[0] : string.trim().split(",");
	}

	private static String toString(String[] array) {
		StringBuilder sb = new StringBuilder();
		for (String string : array) {
			if (sb.length() > 0) { sb.append(","); }
			sb.append(string);
		}
		return sb.toString();
	}
	
	public Map<String, Object> toJSONFromPigStats(PigStats pigStats) {
		Map<String, Object> pigStatsGroup = new HashMap<String, Object>();

		
		// pig summary related counters
		pigStatsGroup.put("BYTES_WRITTEN", Long.toString(pigStats.getBytesWritten()));
		pigStatsGroup.put("DURATION", Long.toString(pigStats.getDuration()));
		pigStatsGroup.put("ERROR_CODE", Long.toString(pigStats.getErrorCode()));
		pigStatsGroup.put("ERROR_MESSAGE", pigStats.getErrorMessage());
		pigStatsGroup.put("FEATURES", pigStats.getFeatures());
		pigStatsGroup.put("HADOOP_VERSION", pigStats.getHadoopVersion());
		pigStatsGroup.put("NUMBER_JOBS", Long.toString(pigStats.getNumberJobs()));
		pigStatsGroup.put("PIG_VERSION", pigStats.getPigVersion());
		pigStatsGroup.put("PROACTIVE_SPILL_COUNT_OBJECTS", Long.toString(pigStats.getProactiveSpillCountObjects()));
		pigStatsGroup.put("PROACTIVE_SPILL_COUNT_RECORDS", Long.toString(pigStats.getProactiveSpillCountRecords()));
		pigStatsGroup.put("RECORD_WRITTEN", Long.toString(pigStats.getRecordWritten()));
		pigStatsGroup.put("RETURN_CODE", Long.toString(pigStats.getReturnCode()));
		pigStatsGroup.put("SCRIPT_ID", pigStats.getScriptId());
		pigStatsGroup.put("SMM_SPILL_COUNT", Long.toString(pigStats.getSMMSpillCount()));
		
		PigStats.JobGraph jobGraph = pigStats.getJobGraph();
		StringBuffer sb = new StringBuffer();
		String separator = ",";

		for (JobStats jobStats : jobGraph) {
			// Get all the HadoopIds and put them as comma separated string for JOB_GRAPH
			String hadoopId = jobStats.getJobId();
			if (sb.length() > 0) {
				sb.append(separator);
			}
			sb.append(hadoopId);
			// Hadoop Counters for pig created MR job
			pigStatsGroup.put(hadoopId, toJSONFromJobStats(jobStats));
		}
		pigStatsGroup.put("JOB_GRAPH", sb.toString());
		return pigStatsGroup;
	}

	public Map<String, Object> toJSONFromJobStats(JobStats jobStats) {
		Map<String, Object> jobStatsGroup = new HashMap<String, Object>();

		// hadoop counters
		jobStatsGroup.put(PigStatsUtil.HDFS_BYTES_WRITTEN, Long.toString(jobStats.getHdfsBytesWritten()));
		jobStatsGroup.put(PigStatsUtil.MAP_INPUT_RECORDS, Long.toString(jobStats.getMapInputRecords()));
		jobStatsGroup.put(PigStatsUtil.MAP_OUTPUT_RECORDS, Long.toString(jobStats.getMapOutputRecords()));
		jobStatsGroup.put(PigStatsUtil.REDUCE_INPUT_RECORDS, Long.toString(jobStats.getReduceInputRecords()));
		jobStatsGroup.put(PigStatsUtil.REDUCE_OUTPUT_RECORDS, Long.toString(jobStats.getReduceOutputRecords()));
		// currently returns null; pig bug
		jobStatsGroup.put("HADOOP_COUNTERS", toJSONFromCounters(jobStats.getHadoopCounters()));
		
		// pig generated hadoop counters and other stats
		jobStatsGroup.put("Alias", jobStats.getAlias());
		jobStatsGroup.put("AVG_MAP_TIME", Long.toString(jobStats.getAvgMapTime()));
		jobStatsGroup.put("AVG_REDUCE_TIME", Long.toString(jobStats.getAvgREduceTime()));
		jobStatsGroup.put("BYTES_WRITTEN", Long.toString(jobStats.getBytesWritten()));
		jobStatsGroup.put("ERROR_MESSAGE", jobStats.getErrorMessage());
		jobStatsGroup.put("FEATURE", jobStats.getFeature());
		jobStatsGroup.put("JOB_ID", jobStats.getJobId());
		jobStatsGroup.put("MAX_MAP_TIME", Long.toString(jobStats.getMaxMapTime()));
		jobStatsGroup.put("MIN_MAP_TIME", Long.toString(jobStats.getMinMapTime()));
		jobStatsGroup.put("MAX_REDUCE_TIME", Long.toString(jobStats.getMaxReduceTime()));
		jobStatsGroup.put("MIN_REDUCE_TIME", Long.toString(jobStats.getMinReduceTime()));
		jobStatsGroup.put("NUMBER_MAPS", Long.toString(jobStats.getNumberMaps()));
		jobStatsGroup.put("NUMBER_REDUCES", Long.toString(jobStats.getNumberReduces()));
		jobStatsGroup.put("PROACTIVE_SPILL_COUNT_OBJECTS", Long.toString(jobStats.getProactiveSpillCountObjects()));
		jobStatsGroup.put("PROACTIVE_SPILL_COUNT_RECS", Long.toString(jobStats.getProactiveSpillCountRecs()));
		jobStatsGroup.put("RECORD_WRITTEN", Long.toString(jobStats.getRecordWrittern()));
		jobStatsGroup.put("SMMS_SPILL_COUNT", Long.toString(jobStats.getSMMSpillCount()));
		jobStatsGroup.put("MULTI_STORE_COUNTERS", toJSONFromMultiStoreCounters(jobStats.getMultiStoreCounters()));

		return jobStatsGroup;
	}
	
	public Map<String, Object> toJSONFromCounters(Counters counters) {
		if (counters == null) {
			return null;
		}

		Map<String, Object> groups = new HashMap<String, Object>();
		for (String gName : counters.getGroupNames()) {
			Map<String, Object> group = new HashMap<String, Object>();
			for (Counter counter : counters.getGroup(gName)) {
				String cName = counter.getName();
				Long cValue = counter.getValue();
				group.put(cName, Long.toString(cValue));
			}
			groups.put(gName, group);
		}
		return groups;
	}
	
	public Map<String, Object> toJSONFromMultiStoreCounters(Map<String, Long> map) {
		Map<String, Object> group = new HashMap<String, Object>();
		for (String cName : map.keySet()) {
			group.put(cName, map.get(cName));
		}
		return group;
	}

}
