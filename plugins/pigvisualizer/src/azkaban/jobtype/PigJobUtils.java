package azkaban.jobtype;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;

import azkaban.utils.JSONUtils;

public class PigJobUtils {
	
	public static String compileJobStats(JobStats jobStats) throws IOException {
		Map<String, String> stats = new HashMap<String, String>();
		JobID jobId = JobID.forName(jobStats.getJobId());
		JobClient jobClient = getJobClient();
		RunningJob runningJob = jobClient.getJob(jobId);
		//JobStatus jobStatus = runningJob.getJobStatus(); this comes after hadoop 1.2.1
		stats.put("jobName", runningJob.getJobName());
		stats.put("trackingURL", runningJob.getTrackingURL());
		stats.put("isComplete", String.valueOf(runningJob.isComplete()));
		stats.put("failureInfo", runningJob.getFailureInfo());
		stats.put("isSuccessful", String.valueOf(runningJob.isSuccessful()));

		// job counters
		Counters counters = jobStats.getHadoopCounters();
		for(String groupName : counters.getGroupNames()) {
			Map<String, String> counterStats = new HashMap<String, String>();
			Group group = counters.getGroup(groupName);
			Iterator<Counters.Counter> iter = group.iterator();
			while(iter.hasNext()) {
				Counter counter = iter.next();
				counterStats.put(counter.getDisplayName(), String.valueOf(counter.getCounter()));
			}
			stats.put(groupName, JSONUtils.toJSON(counterStats));
		}
		
		stats.put("alias", jobStats.getAlias());
		stats.put("feature", jobStats.getFeature());
		
		// run time
		stats.put("numberMaps", String.valueOf(jobStats.getNumberMaps()));
		
		stats.put("maxMapTime", String.valueOf(jobStats.getMaxMapTime()));
		stats.put("minMapTime", String.valueOf(jobStats.getMinMapTime()));
		stats.put("avgMapTime", String.valueOf(jobStats.getAvgMapTime()));
		
		stats.put("numberReduces", String.valueOf(jobStats.getNumberReduces()));
		
		stats.put("maxReduceTime", String.valueOf(jobStats.getMaxReduceTime()));
		stats.put("minReduceTime", String.valueOf(jobStats.getMinReduceTime()));
		stats.put("avgReduceTime", String.valueOf(jobStats.getAvgREduceTime()));
		
		// inputs		
		List<InputStats> inputStats = jobStats.getInputs();
		Map<String, String> inputs = new HashMap<String, String>();
		for(InputStats inputStat : inputStats) {
			Map<String, String> input = new HashMap<String, String>();
			input.put("name", inputStat.getName());
			input.put("location", inputStat.getLocation());
			input.put("bytes", String.valueOf(inputStat.getBytes()));
			input.put("numberRecords", String.valueOf(inputStat.getNumberRecords()));
			inputs.put(inputStat.getName(), JSONUtils.toJSON(input));
		}
		stats.put("inputs", JSONUtils.toJSON(inputs));
		
		// outputs
		List<OutputStats> outputStats = jobStats.getOutputs();
		Map<String, String> outputs = new HashMap<String, String>();
		for(OutputStats outputStat : outputStats) {
			Map<String, String> output = new HashMap<String, String>();
			output.put("name", outputStat.getName());
			output.put("location", outputStat.getLocation());
			output.put("bytes", String.valueOf(outputStat.getBytes()));
			output.put("numberRecords", String.valueOf(outputStat.getNumberRecords()));
			outputs.put(outputStat.getName(), JSONUtils.toJSON(outputStat));
			// should we have sample output data here?
		}
		stats.put("outputs", JSONUtils.toJSON(outputs));
		
		stats.put("mapInputRecords", String.valueOf(jobStats.getMapInputRecords()));
		stats.put("mapOutputRecords", String.valueOf(jobStats.getMapOutputRecords()));
		stats.put("bytesWritten", String.valueOf(jobStats.getBytesWritten()));
		stats.put("hdfsBytesWritten", String.valueOf(jobStats.getHdfsBytesWritten()));
		stats.put("recordsWritten", String.valueOf(jobStats.getRecordWrittern()));
		stats.put("reduceInputRecords", String.valueOf(jobStats.getReduceInputRecords()));
		stats.put("reduceOutputRecords", String.valueOf(jobStats.getReduceOutputRecords()));
		stats.put("smmSpillCount", String.valueOf(jobStats.getSMMSpillCount()));
		
		stats.put("errorMessage", jobStats.getErrorMessage());
		
		return JSONUtils.toJSON(stats);
	}
	
	public static String compileTaskStats(RunningJob job) throws IOException {
		JobID jobId = job.getID();
		JobClient jobClient = getJobClient();
		Map<String, String> stats = new HashMap<String, String>();
		TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobId);
		for(TaskReport report : mapTaskReport) {

		}
		TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobId);
		for(TaskReport report : reduceTaskReport) {
			
		}
		return JSONUtils.toJSON(stats);
	}
	
	private static JobClient getJobClient() {
		return new JobClient();
	}
}
