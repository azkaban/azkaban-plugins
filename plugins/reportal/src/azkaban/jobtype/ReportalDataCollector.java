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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import azkaban.flow.CommonJobProperties;
import azkaban.reportal.util.CompositeException;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.ReportalUtil;
import azkaban.utils.Props;

public class ReportalDataCollector extends ReportalAbstractRunner {

	Props prop;

	public ReportalDataCollector(String jobName, Properties props) {
		super(props);
		prop = new Props();
		prop.put(props);
	}

	@Override
	protected void runReportal() throws Exception {
		System.out.println("Reportal Data Collector: Initializing");

		String outputFileSystem = props.getString("reportal.output.filesystem", "local");
		String outputBase = props.getString("reportal.output.location", "/tmp/reportal");
		String execId = props.getString(CommonJobProperties.EXEC_ID);

		int jobNumber = prop.getInt("reportal.job.number");
		List<Exception> exceptions = new ArrayList<Exception>();
		for (int i = 0; i < jobNumber; i++) {
			try {
				String jobTitle = prop.getString("reportal.job." + i);
				System.out.println("Reportal Data Collector: Job name=" + jobTitle);

				String subPath = "/" + execId + "/" + jobTitle + ".csv";
				String locationFull = (outputBase + subPath).replace("//", "/");
				String locationTemp = ("./reportal/" + jobTitle + ".csv").replace("//", "/");
				File tempOutput = new File(locationTemp);
				if(!tempOutput.exists()) {
					throw new FileNotFoundException("File: " + tempOutput.getAbsolutePath() + " does not exist.");
				}

				// Copy file to persistent saving location
				System.out.println("Reportal Data Collector: Saving output to persistent storage");
				System.out.println("Reportal Data Collector: FS=" + outputFileSystem + ", Location=" + locationFull);
				// Open temp file
				InputStream tempStream = new BufferedInputStream(new FileInputStream(tempOutput));
				// Open file from HDFS if specified
				IStreamProvider outputProvider = ReportalUtil.getStreamProvider(outputFileSystem);
				OutputStream persistentStream = outputProvider.getFileOutputStream(locationFull);
				// Copy it
				IOUtils.copy(tempStream, persistentStream);
				tempStream.close();
				persistentStream.close();
				outputProvider.cleanUp();
			} catch (Exception e) {
				System.out.println("Reportal Data Collector: Data collection failed. " + e.getMessage());
				e.printStackTrace();
				exceptions.add(e);
			}
		}

		if (exceptions.size() > 0) {
			throw new CompositeException(exceptions);
		}

		System.out.println("Reportal Data Collector: Ended successfully");
	}

	@Override
	protected boolean requiresOutput() {
		return false;
	}
}
