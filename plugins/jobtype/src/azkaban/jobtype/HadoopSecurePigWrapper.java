package azkaban.jobtype;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.security.HadoopSecurityManager;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class HadoopSecurePigWrapper {

	public static void main(final String[] args) throws IOException, InterruptedException, RuntimeException {
		final Logger logger = Logger.getRootLogger();
		final Properties p = System.getProperties();
		final Configuration conf = new Configuration();
		
		String filelocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
		if(filelocation == null) {
			throw new RuntimeException("hadoop token information not set.");
		}		
		if(!new File(filelocation).exists()) {
			throw new RuntimeException("hadoop token file doesn't exist.");			
		}
		
		logger.info("Found token file " + filelocation);
//		logger.info("Security enabled is " + UserGroupInformation.isSecurityEnabled());
		
		logger.info("Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY + " to " + filelocation);
		System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, filelocation);
		
		UserGroupInformation loginUser = null;
		UserGroupInformation proxyUser = null;

		//logger.info("Proxying enabled.");
		UserGroupInformation.setConfiguration(conf);
			
		loginUser = UserGroupInformation.getLoginUser();
		logger.info("Current logged in user is " + loginUser.getUserName());
		
		String userToProxy = p.getProperty("user.to.proxy");
		logger.info("Creating proxy user.");
		proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);

		for (Token<?> token: loginUser.getTokens()) {
			proxyUser.addToken(token);
		}
		proxyUser.doAs(
				new PrivilegedExceptionAction<Void>() {
					@Override
					public Void run() throws Exception {
						//prefetchToken();
						org.apache.pig.Main.main(args);
						return null;
					}
				});
	}
}

