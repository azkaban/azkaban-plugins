package azkaban.viewer.reportal;

import java.io.File;
import java.util.Map;

import azkaban.flow.CommonJobProperties;
import azkaban.reportal.util.Reportal;
import azkaban.utils.Props;

public class ReportalTypeManager {
	public static final String DATA_COLLECTOR_JOB = "ReportalDataCollector";
	public static final String DATA_COLLECTOR_JOB_TYPE = "reportaldatacollector";

	public static void createJobAndFiles(Reportal reportal, File jobFile, String jobName, String queryTitle, String queryType,
			String queryScript, String dependentJob, String userName, Map<String, String> extras) throws Exception {

		// Create props for the job
		Props propertiesFile = new Props();
		propertiesFile.put("title", queryTitle);

		ReportalType type = ReportalType.getTypeByName(queryType);

		if (type == null) {
			throw new Exception("Type " + queryType + " is invalid.");
		}

		propertiesFile.put("reportal.title", reportal.title);
		propertiesFile.put("reportal.job.title", jobName);
		propertiesFile.put("reportal.job.query", queryScript);
		if (userName != null) {
			propertiesFile.put("user.to.proxy", "${reportal.execution.user}");
			propertiesFile.put("reportal.proxy.user", userName);
		}

		type.buildJobFiles(reportal, propertiesFile, jobFile, jobName, queryScript, userName);

		propertiesFile.put(CommonJobProperties.JOB_TYPE, type.getJobTypeName());

		// Order dependency
		if (dependentJob != null) {
			propertiesFile.put(CommonJobProperties.DEPENDENCIES, dependentJob);
		}

		if (extras != null) {
			propertiesFile.putAll(extras);
		}

		propertiesFile.storeLocal(jobFile);
	}
}
