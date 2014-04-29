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

import azkaban.utils.Props;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class HadoopJavaJobRunnerMain2 extends AbstractHadoopJavaMain{

	public static final String DEFAULT_RUN_METHOD = "main";
	public static final String DEFAULT_CANCEL_METHOD = "cancel";

	private final Class<?> runningClass;

	public static void main(String[] args) throws Exception {
		@SuppressWarnings("unused")
		HadoopJavaJobRunnerMain2 wrapper = new HadoopJavaJobRunnerMain2(args);
	}

	public HadoopJavaJobRunnerMain2(String[] args) throws Exception {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				cancelJob();
			}
		});

		try {
			setup();

			logger.info("Running job " + jobName);
			
			String className = prop.getProperty(JOB_CLASS);
			if (className == null) {
				throw new Exception("Class name is not set.");
			}
			logger.info("Class name " + className);
			
			UserGroupInformation userUGI = getUserUGI();
			
			runningClass = getClass(className);
			
			_cancelMethod = prop.getProperty(CANCEL_METHOD_PARAM, DEFAULT_CANCEL_METHOD);
			_runMethod = prop.getProperty(RUN_METHOD_PARAM, DEFAULT_RUN_METHOD);

			logger.info("Invoking method " + _runMethod + " from " + runningClass.getName());

			runMethodAsUser(userUGI, args);

			isFinished = true;

			// Get the generated properties and store them to disk, to be read
			// by ProcessJob.
			try {
				final Method generatedPropertiesMethod = runningClass.getMethod(
						GET_GENERATED_PROPERTIES_METHOD, new Class<?>[] {});
				Object outputGendProps = generatedPropertiesMethod.invoke(null, new Object[] {});
				final Method toPropertiesMethod = outputGendProps.getClass().getMethod("toProperties", new Class<?>[] {});
				Properties properties = (Properties)toPropertiesMethod.invoke(outputGendProps, new Object[] {});

				Props outputProps = new Props(null, properties);
				outputGeneratedProperties(outputProps);
			} catch (NoSuchMethodException e) {
				logger.info(String.format(
						"Apparently there isn't a static method[%s], using empty Props object instead.",
						GET_GENERATED_PROPERTIES_METHOD));
				outputGeneratedProperties(new Props());
			}
		} catch (Exception e) {
			isFinished = true;
			throw e;
		}
	}

	private void runMethodAsUser(final UserGroupInformation ugi, final String[] args) throws IOException, InterruptedException {
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() throws Exception {
				Configuration conf = new Configuration();
				if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
					conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
				}
				
				runMethod(args);
				return null;
			}
		});
	}

	private void runMethod(String[] args) throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		runningClass.getMethod(_runMethod, String[].class).invoke(null, new Object[] {args});
	}

	public void cancelJob() {
		if (isFinished) {
			return;
		}
		logger.info("Attempting to call cancel on this job");

		Method cancelMethod = null;

		try {
			cancelMethod = runningClass.getMethod(_cancelMethod);
		} catch (SecurityException e) {
		} catch (NoSuchMethodException e) {
		}

		if (cancelMethod != null)
			try {
				cancelMethod.invoke(null);
			} catch (Exception e) {
				if (logger != null) {
					logger.error("Cancel method failed! ", e);
				}
			}
		else {
			throw new RuntimeException("Job " + jobName + " does not have cancel method " + _cancelMethod);
		}
	}

}
