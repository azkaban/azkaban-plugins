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

import azkaban.utils.JSONUtils;

import com.sun.tools.doclets.internal.toolkit.Configuration;

public class MRJobUtils {
	
	public static RunningJob getRunningJob(String jobId) throws IOException {
		JobClient jobClient = getJobClient();
		return jobClient.getJob(JobID.forName(jobId));
	}
	
	public static String compileJobStats(RunningJob job) throws IOException {
		Map<String, String> stats = new HashMap<String, String>();
		String jobName = job.getJobName();
		stats.put("jobName", jobName);
		String trackingURL = job.getTrackingURL();
		stats.put("trackingURL", trackingURL);
		Counters counters = job.getCounters();
		Collection<String> counterGroups = counters.getGroupNames();
		for(String groupName : counterGroups) {
			Map<String, String> counterStats = new HashMap<String, String>();
			Group group = counters.getGroup(groupName);
			Iterator<Counters.Counter> iter = group.iterator();
			while(iter.hasNext()) {
				Counter counter = iter.next();
				counterStats.put(counter.getDisplayName(), String.valueOf(counter.getCounter()));
			}
			stats.put(groupName, JSONUtils.toJSON(counterStats));
		}
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
