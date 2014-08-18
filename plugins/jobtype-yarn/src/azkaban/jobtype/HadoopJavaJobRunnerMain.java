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

import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class HadoopJavaJobRunnerMain{

	private static boolean securityEnabled = false;
	private static Properties prop = new Properties();
	protected final static Logger logger = Logger.getLogger(HadoopJavaJobRunnerMain.class);

	protected static final Layout DEFAULT_LAYOUT = new PatternLayout("%p %m\n");
	
	private static boolean useToken = false;
	private static boolean useKeytab = false;
	private static boolean shouldProxy = false;
	private static String userToProxy;
	
	public static void main(final String[] args) throws Exception {
		setup();
		UserGroupInformation userUGI = getUserUGI();
		userUGI.doAs(new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() throws Exception {
				Configuration conf = new Configuration();
				if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
					conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
				}
				@SuppressWarnings("unused")
				JavaJobRunnerMain wrapper = new JavaJobRunnerMain(args);
				return null;
			}
		});
	}
	
	protected static void setup() throws FileNotFoundException, IOException {
		
		String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
		prop.load(new BufferedReader(new FileReader(propsFile)));
		// default way to talk to secure hadoop
		useToken = Boolean.valueOf(prop.getProperty("use.token", "true"));
		// allow user job wrapper to use keytab login. potentially unsecure
		useKeytab = Boolean.valueOf(prop.getProperty("use.keytab", "false"));
		shouldProxy = Boolean.valueOf(prop.getProperty(HadoopSecurityManager.ENABLE_PROXYING, "true"));
		userToProxy = prop.getProperty("user.to.proxy");
		
		final Configuration conf = new Configuration();
		UserGroupInformation.setConfiguration(conf);
		securityEnabled = UserGroupInformation.isSecurityEnabled();
	}
	
	protected static UserGroupInformation getUserUGI() throws Exception {
		UserGroupInformation userUGI = null;
		if(shouldProxy == false) {
			return UserGroupInformation.getCurrentUser();
		}
		logger.info("Proxying enabled.");
		if(securityEnabled == true) {
			logger.info("Hadoop Security enabled.");
			if(useToken == true) {
				String filelocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
				if(filelocation == null) {
					throw new Exception("Hadoop token file location must be set!");
				}
				logger.info("Found token file " + filelocation);
				logger.info("Setting mapreduce.job.credentials.binary to " + filelocation);
				System.setProperty("mapreduce.job.credentials.binary", filelocation);

				UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
				logger.info("Current logged in user is " + loginUser.getUserName());
				userUGI = UserGroupInformation.createProxyUser(userToProxy, loginUser);
				for (Token<?> token: loginUser.getTokens()) {
					userUGI.addToken(token);
				}
			} else if(useKeytab == true){
				String keytab = prop.getProperty(HadoopSecurityManager.AZKABAN_KEYTAB_LOCATION);
				String principal = prop.getProperty(HadoopSecurityManager.AZKABAN_PRINCIPAL);
				if(keytab == null || principal == null) {
					throw new Exception("Kerberos login info missing: keytab " + keytab + " principal " + principal);
				}
				logger.setLevel(Level.ERROR);
				UserGroupInformation.loginUserFromKeytab(principal, keytab);
				logger.setLevel(Level.INFO);
				userUGI = UserGroupInformation.createProxyUser(userToProxy, UserGroupInformation.getLoginUser());
			} else {
				throw new Exception("No credentials provided for secure hadoop.");
			}
		} else {
			userUGI = UserGroupInformation.createRemoteUser(userToProxy);
		}
		logger.info("Proxied as user " + userToProxy);
		return userUGI;
	}

}
