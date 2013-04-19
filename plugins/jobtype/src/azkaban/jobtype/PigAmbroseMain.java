package azkaban.jobtype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;

import azkaban.security.commons.HadoopSecurityManager;

public class PigAmbroseMain {
	
	private static Class pigRunner;
	private static boolean shouldProxy;
	private static File pigLogFile;
	private static File metaDataFile;
	private static PigProgressNotificationListener listener;
	
	public static void main(final String[] args) throws Exception {
		final Logger logger = Logger.getRootLogger();
		final Properties p = System.getProperties();
		final Configuration conf = new Configuration();
		
		// PigRunner must be present
		pigRunner = Class.forName("org.apache.pig.PigRunner");
		shouldProxy = System.getenv("azkaban.should.proxy").equals("true") ? true : false;
		pigLogFile = new File(System.getenv("PIG_LOG_FILE"));
		if(System.getenv("OUTPUT_METADATA_FILE") != null) {
			metaDataFile = new File(System.getenv("OUTPUT_METADATA_FILE"));
			listener = new AmbrosePigProgressNotificationListener(metaDataFile);
		}
		else {
			listener = new AmbrosePigProgressNotificationListener();
		}
		
		
		if(shouldProxy) {
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
		else {
			runPigJob(args);
		}
		
		
	}
	
	public static void runPigJob(String[] args) throws Exception {
		
		PigStats stats = PigRunner.run(args, listener);
		if (!stats.isSuccessful()) {
			if (pigLogFile != null) {
				handleError(pigLogFile);
			}
			throw new RuntimeException("Pig job failed.");
		}
		else {

		}
		
	}
	
	private static void handleError(File pigLog) throws Exception {
		System.err.println();
		System.err.println("Pig logfile dump:");
		System.err.println();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(pigLog));
			String line = reader.readLine();
			while (line != null) {
				System.err.println(line);
				line = reader.readLine();
			}
			reader.close();
		}
		catch (FileNotFoundException e) {
			System.err.println("pig log file: " + pigLog + "  not found.");
		}
	}

}
