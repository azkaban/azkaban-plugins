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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HadoopJavaJobRunnerMain extends AbstractHadoopJavaMain{

	public static final String DEFAULT_RUN_METHOD = "run";
	public static final String DEFAULT_CANCEL_METHOD = "cancel";

	public Object _javaObject;

	public static void main(String[] args) throws Exception {
		@SuppressWarnings("unused")
		HadoopJavaJobRunnerMain wrapper = new HadoopJavaJobRunnerMain();
	}

	public HadoopJavaJobRunnerMain() throws Exception {
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
			
			_javaObject = getObjectAsProxyUser(prop, logger, jobName, className, userUGI);
			
			logger.info("Got object " + _javaObject.toString());
	
			if (_javaObject == null) {
				logger.info("Could not create java object to run job: " + className);
				throw new Exception("Could not create running object");
			}

			_cancelMethod = prop.getProperty(CANCEL_METHOD_PARAM, DEFAULT_CANCEL_METHOD);

			final String runMethod = prop.getProperty(RUN_METHOD_PARAM, DEFAULT_RUN_METHOD);
			logger.info("Invoking method " + runMethod);

			runMethodAsUser(prop, _javaObject, runMethod, userUGI);
			
			isFinished = true;

			// Get the generated properties and store them to disk, to be read
			// by ProcessJob.
			try {
				final Method generatedPropertiesMethod = _javaObject.getClass().getMethod(
						GET_GENERATED_PROPERTIES_METHOD, new Class<?>[] {});
				Object outputGendProps = generatedPropertiesMethod.invoke(_javaObject, new Object[] {});
				final Method toPropertiesMethod = outputGendProps.getClass().getMethod("toProperties", new Class<?>[] {});
				Properties properties = (Properties)toPropertiesMethod.invoke(outputGendProps, new Object[] {});

				Props outputProps = new Props(null, properties);
				outputGeneratedProperties(outputProps);
			} catch (NoSuchMethodException e) {
				logger.info(String.format(
						"Apparently there isn't a method[%s] on object[%s], using empty Props object instead.",
						GET_GENERATED_PROPERTIES_METHOD, _javaObject));
				outputGeneratedProperties(new Props());
			}
		} catch (Exception e) {
			isFinished = true;
			throw e;
		}
	}

	private void runMethodAsUser(Properties prop, final Object obj, final String runMethod, final UserGroupInformation ugi) throws IOException,
			InterruptedException {
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() throws Exception {
				
				Configuration conf = new Configuration();
				if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
					conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
				}
				
				runMethod(obj, runMethod);
				return null;
			}
		});
	}

	private void runMethod(Object obj, String runMethod) throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		obj.getClass().getMethod(runMethod, new Class<?>[] {}).invoke(obj);
	}

	public void cancelJob() {
		if (isFinished) {
			return;
		}
		logger.info("Attempting to call cancel on this job");
		if (_javaObject != null) {
			Method method = null;

			try {
				method = _javaObject.getClass().getMethod(_cancelMethod);
			} catch (SecurityException e) {
			} catch (NoSuchMethodException e) {
			}

			if (method != null)
				try {
					method.invoke(_javaObject);
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

	private static Object getObjectAsProxyUser(final Properties prop, final Logger logger, final String jobName,
			final String className, final UserGroupInformation ugi) throws Exception {

		Object obj = ugi.doAs(
		new PrivilegedExceptionAction<Object>() {
			@Override
			public Object run() throws Exception {
				return getObject(jobName, className, prop, logger);
			}
		});

		return obj;
	}

	private static Object getObject(String jobName, String className, Properties properties, Logger logger)
			throws Exception {

		Class<?> runningClass = HadoopJavaJobRunnerMain.class.getClassLoader().loadClass(className);

		if (runningClass == null) {
			throw new Exception("Class " + className + " was not found. Cannot run job.");
		}

		Class<?> propsClass = null;
		for (String propClassName : PROPS_CLASSES) {
			try {
				propsClass = HadoopJavaJobRunnerMain.class.getClassLoader().loadClass(propClassName);
			}
			catch (ClassNotFoundException e) {
			}
			
			if (propsClass != null && getConstructor(runningClass, String.class, propsClass) != null) {
				//is this the props class 
				break;
			}
			propsClass = null;
		}

		Object obj = null;
		if (propsClass != null && getConstructor(runningClass, String.class, propsClass) != null) {
			// Create props class
			Constructor<?> propsCon = getConstructor(propsClass, propsClass, Properties[].class);
			Object props = propsCon.newInstance(null, new Properties[] { properties });

			Constructor<?> con = getConstructor(runningClass, String.class, propsClass);
			logger.info("Constructor found " + con.toGenericString());
			obj = con.newInstance(jobName, props);
		} else if (getConstructor(runningClass, String.class, Properties.class) != null) {
			
			Constructor<?> con = getConstructor(runningClass, String.class, Properties.class);
			logger.info("Constructor found " + con.toGenericString());
			obj = con.newInstance(jobName, properties);
		} else if (getConstructor(runningClass, String.class, Map.class) != null) {
			Constructor<?> con = getConstructor(runningClass, String.class, Map.class);
			logger.info("Constructor found " + con.toGenericString());

			HashMap<Object, Object> map = new HashMap<Object, Object>();
			for (Map.Entry<Object, Object> entry : properties.entrySet()) {
				map.put(entry.getKey(), entry.getValue());
			}
			obj = con.newInstance(jobName, map);
		} else if (getConstructor(runningClass, String.class) != null) {
			Constructor<?> con = getConstructor(runningClass, String.class);
			logger.info("Constructor found " + con.toGenericString());
			obj = con.newInstance(jobName);
		} else if (getConstructor(runningClass) != null) {
			Constructor<?> con = getConstructor(runningClass);
			logger.info("Constructor found " + con.toGenericString());
			obj = con.newInstance();
		} else {
			logger.error("Constructor not found. Listing available Constructors.");
			for (Constructor<?> c : runningClass.getConstructors()) {
				logger.info(c.toGenericString());
			}
		}
		return obj;
	}

	private static Constructor<?> getConstructor(Class<?> c, Class<?>... args) {
		try {
			Constructor<?> cons = c.getConstructor(args);
			return cons;
		} catch (NoSuchMethodException e) {
			return null;
		}
	}

}
