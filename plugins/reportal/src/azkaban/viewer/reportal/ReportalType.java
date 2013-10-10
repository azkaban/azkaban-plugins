package azkaban.viewer.reportal;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;

import azkaban.reportal.util.Reportal;
import azkaban.user.User;
import azkaban.utils.Props;

public enum ReportalType {

	PigJob("ReportalPig", "reportalpig", "hadoop") {
		@Override
		public void buildJobFiles(Reportal reportal, Props propertiesFile, File jobFile, String jobName, String queryScript, String proxyUser) {
			File resFolder = new File(jobFile.getParentFile(), "res");
			resFolder.mkdirs();
			File scriptFile = new File(resFolder, "script.pig");

			OutputStream fileOutput = null;
			try {
				scriptFile.createNewFile();
				fileOutput = new BufferedOutputStream(new FileOutputStream(scriptFile));
				fileOutput.write(queryScript.getBytes(Charset.forName("UTF-8")));
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			finally {
				if (fileOutput != null) {
					try {
						fileOutput.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			propertiesFile.put("reportal.pig.script", "res/script.pig");

			// propertiesFile.put("reportal.job.query", queryScript);
		}
	},
	HiveJob("ReportalHive", "reportalhive", "hadoop"),
	TeraDataJob("ReportalTeraData", "reportalteradata", "teradata"),
	DataCollectorJob(ReportalTypeManager.DATA_COLLECTOR_JOB, ReportalTypeManager.DATA_COLLECTOR_JOB_TYPE, "") {
		@Override
		public void buildJobFiles(Reportal reportal, Props propertiesFile, File jobFile, String jobName, String queryScript, String proxyUser) {
			propertiesFile.put("user.to.proxy", proxyUser);
		}
	};

	private String typeName;
	private String jobTypeName;
	private String permissionName;

	private ReportalType(String typeName, String jobTypeName, String permissionName) {
		this.typeName = typeName;
		this.jobTypeName = jobTypeName;
		this.permissionName = permissionName;
	}

	public void buildJobFiles(Reportal reportal, Props propertiesFile, File jobFile, String jobName, String queryScript, String proxyUser) {

	}

	public String getJobTypeName() {
		return jobTypeName;
	}

	private static HashMap<String, ReportalType> reportalTypes = new HashMap<String, ReportalType>();

	static {
		for (ReportalType type: ReportalType.values()) {
			reportalTypes.put(type.typeName, type);
		}
	}

	public static ReportalType getTypeByName(String typeName) {
		return reportalTypes.get(typeName);
	}

	public boolean checkPermission(User user) {
		return user.hasPermission(permissionName);
	}

	@Override
	public String toString() {
		return typeName;
	}
}
