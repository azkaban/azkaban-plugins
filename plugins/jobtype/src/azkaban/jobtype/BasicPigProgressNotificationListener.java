package azkaban.jobtype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;

public class BasicPigProgressNotificationListener implements PigProgressNotificationListener {
	protected Logger logger = Logger.getLogger(BasicPigProgressNotificationListener.class);
	private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();
//	private Map<String, DAGNode> dagNodeNameMap = new TreeMap<String, DAGNode>();

	private HashSet<String> completedJobIds = new HashSet<String>();

	protected static enum WorkflowProgressField {
		workflowProgress;
	}

	protected static enum JobProgressField {
		jobId, jobName, trackingUrl, isComplete, isSuccessful,
		mapProgress, reduceProgress, totalMappers, totalReducers;
	}

	/**
	 * Called after the job DAG has been created, but before any jobs are fired.
	 * @param plan the MROperPlan that represents the DAG of operations. Each operation will become
	 * a MapReduce job when it's launched.
	 */
	@Override
	public void initialPlanNotification(String scriptId, MROperPlan plan) {
		Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();
		
		System.out.println(scriptId);
		System.out.println(plan);
		
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

		System.out.println(scriptId);
		System.out.println(assignedJobId);
		System.out.println(jobGraph);
	}

	/**
	 * Called when a job fails. Mark the job as failed and push a status event.
	 * @param scriptId scriptId of the running script
	 * @param stats JobStats for the failed job.
	 */
	@Override
	public void jobFailedNotification(String scriptId, JobStats stats) {

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
	public void outputCompletedNotification(String scriptId, OutputStats outputStats) { }

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
}


