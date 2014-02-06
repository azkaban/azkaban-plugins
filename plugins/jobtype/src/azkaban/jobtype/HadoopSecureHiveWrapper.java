/*
 * Copyright 2012 LinkedIn, Inc
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class HadoopSecureHiveWrapper {
	
	private static boolean securityEnabled;
	
	public static void main(final String[] args) throws Exception {
		
		final Logger logger = Logger.getRootLogger();

		String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
		Properties prop = new Properties();
		prop.load(new BufferedReader(new FileReader(propsFile)));

		final Configuration conf = new Configuration();
		
		UserGroupInformation.setConfiguration(conf);
		securityEnabled = UserGroupInformation.isSecurityEnabled();

		if (shouldProxy(prop)) {
			UserGroupInformation proxyUser = null;
			String userToProxy = prop.getProperty("user.to.proxy");
			if(securityEnabled) {
				String filelocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
				if(filelocation == null) {
					throw new RuntimeException("hadoop token information not set.");
				}		
				if(!new File(filelocation).exists()) {
					throw new RuntimeException("hadoop token file doesn't exist.");			
				}
				
				logger.info("Found token file " + filelocation);

				logger.info("Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to " + filelocation);
				System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, filelocation);
				
				UserGroupInformation loginUser = null;

				loginUser = UserGroupInformation.getLoginUser();
				logger.info("Current logged in user is " + loginUser.getUserName());
				
				logger.info("Creating proxy user.");
				proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);
		
				for (Token<?> token: loginUser.getTokens()) {
					proxyUser.addToken(token);
				}
			}
			else {
				proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
			}
			
			logger.info("Proxied as user " + userToProxy);
			
			proxyUser.doAs(
					new PrivilegedExceptionAction<Void>() {
						@Override
						public Void run() throws Exception {
								runHive(args);
								return null;
						}
					});

		}
		else {
			logger.info("Not proxying. ");
			runHive(args);
		}
	}
	
	public static void runHive(String[] args) throws Exception {
		
		Configuration hiveConf = new Configuration();
		
		//TODO: says so by oozie, not sure if it is true
		// Have to explicitly unset this property or Hive will not set it.
		//hiveConf.set("mapred.job.name", "");

		// See https://issues.apache.org/jira/browse/HIVE-1411
		hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

		// to force hive to use the jobclient to submit the job, never using HADOOPBIN (to do localmode)
		hiveConf.setBoolean("hive.exec.mode.local.auto", false);

		//TODO: logFile, still useful?
		CliDriver.main(args);
	}
	
	public static boolean shouldProxy(Properties prop) {
		String shouldProxy = prop.getProperty(HadoopSecurityManager.ENABLE_PROXYING);

		return shouldProxy != null && shouldProxy.equals("true");
	}
}

