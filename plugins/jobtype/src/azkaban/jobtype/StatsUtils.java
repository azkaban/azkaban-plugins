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
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.impl.util.ObjectSerializer;

public class StatsUtils {
	
  private static Logger logger = Logger.getLogger(StatsUtils.class);

	public static Properties getJobConf(RunningJob runningJob) {
		Properties jobConfProperties = null;
		try {
			Path path = new Path(runningJob.getJobFile());
			Configuration conf = new Configuration(false);
			FileSystem fs = FileSystem.get(new Configuration());
			InputStream in = fs.open(path);
			conf.addResource(in);

			jobConfProperties = new Properties();
			for (Map.Entry<String, String> entry : conf) {
				if (entry.getKey().equals("pig.mapPlan") ||
						entry.getKey().equals("pig.reducePlan")) {
					jobConfProperties.setProperty(entry.getKey(),
							ObjectSerializer.deserialize(entry.getValue()).toString());
				}
				else {
					jobConfProperties.setProperty(entry.getKey(), entry.getValue());
				}
			}
		}
		catch (FileNotFoundException e) {
			logger.warn("Job conf not found.");
		}
		catch (IOException e) {
			logger.warn("Error while retrieving job conf: " + e.getMessage());
		}
		return jobConfProperties;
	}
	
	public static Object propertiesToJson(Properties properties) {
		Map<String, String> jsonObj = new HashMap<String, String>();
		Set<String> keys = properties.stringPropertyNames();
		for (String key : keys) {
			jsonObj.put(key, properties.getProperty(key));
		}
		return jsonObj;
	}

	public static Properties propertiesFromJson(Object obj) {
		Map<String, String> jsonObj = (HashMap<String, String>) obj;
		Properties properties = new Properties();
		for (Map.Entry<String, String> entry : jsonObj.entrySet()) {
			properties.setProperty(entry.getKey(), entry.getValue());
		}
		return properties;
	}
}
