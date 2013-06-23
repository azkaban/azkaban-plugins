package azkaban.triggertype.HdfsDataTrigger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadableDuration;
import org.joda.time.ReadablePeriod;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.trigger.ConditionChecker;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import azkaban.utils.Utils;
import azkaban.utils.cache.Cache;
import azkaban.utils.cache.CacheManager;
import azkaban.utils.cache.Cache.EjectionPolicy;


/*
 * 
 * 	pattern is like this:
 * 
 * 	/user/azkaban/abc/def/${YEAR}/${MONTH}/${DAY}/${HOUR}
 *	
 *	need variables:
 *		YEAR { INIT = 2013 (default NOW.YEAR) INCREMENT = 0 } 	
 *		MONTH { INIT = 6   (default NOW.MONTH) INCREMENT = 0 }
 *		DAY { INIT = 13   (default NOW.DAY) INCREMENT = 1 }
 *		HOUR { INIT = 17   (default NOW.HOUR) INCREMENT = 0 }
 *		TIMEZONE { INIT = America/New_York		(default UTC) INCREMENT = 0 }
 *		/user/azkaban/abc/def/${YEAR}/${MONTH}/${DAY}/${HOUR}
 * 		
 * 		TIME_TO_EXPIRE = 24h
 * 
 * 		hdfsUser 
 * 
 * 	servelet should be supplied with these:
 * 		data source: magic hdfs
 * 		default variable list
 * 		default expire time
 * 
 */

public class HdfsDataChecker implements ConditionChecker{
	
	private static final Logger logger = Logger.getLogger(HdfsDataChecker.class);
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	//private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z_.0-9]+)\\}");
	private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z_.0-9]+)\\}");
	
	private static HadoopSecurityManager hadoopSecurityManager; 
	private static HdfsDataCheckingThread dataCheckingThread;
	
	public static String type;
	private static boolean init = false;
	private static String dataSource;

	private final String fsName;
	private ReadablePeriod timeToExpire = Utils.parsePeriodString("24h");
	private final String checkerId;
	private final String hdfsUser;
	private List<Path> dataPaths;
	private final List<String> dataPathPatterns;
	// for all variables
	// reserved variables: YEAR, MONTH, DAY, HOUR. takes initial value and increment.
	private	Map<String, PathVariable> variables;
	
	public static class PathVariable {
		int value;
		int increment;
		
		public PathVariable(int v, int inc) {
			this.value = v;
			this.increment = inc;
		}
		public int getValue() {
			return value;
		}
		public void setValue(int v) {
			value = v;
		}
		public void increment() {
			value += increment;
		}
		public int getIncrement() {
			return increment;
		}
	}
	
	public HdfsDataChecker(String checkerId, String hdfsUser, List<String> dataCheckPattern, Map<String, PathVariable> variables) throws Exception {
		
		this.dataPathPatterns = dataCheckPattern;
		this.hdfsUser = hdfsUser;
		this.checkerId = checkerId;
		this.variables = variables;
		
		// sanity check
		checkNeededVariables();
		
		fsName = hadoopSecurityManager.getFSAsUser(hdfsUser).getUri().toString();

		dataPaths = new ArrayList<Path>();
		for(String str : dataCheckPattern) {
			dataPaths.add(getPathFromPattern(str));
		}
	}
	
	public String getHdfsUser() {
		return hdfsUser;
	}

	public void setTimeToExpire(ReadablePeriod duration) {
		this.timeToExpire = duration;
	}
	
	private void checkNeededVariables() throws Exception {
		for(String pattern : dataPathPatterns) {
			Matcher matcher = VARIABLE_PATTERN.matcher(pattern);
			while(matcher.find()) {
				if(!variables.containsKey(matcher.group(1))) {
					throw new Exception("The need variable " + matcher.group(1) + " is not specified.");
				}
			}
		}
		DateTime now = DateTime.now();
		if(!variables.containsKey("YEAR")) {
			variables.put("YEAR", new PathVariable(now.getYear(), 0));
		}
		if(!variables.containsKey("MONTH")) {
			variables.put("MONTH", new PathVariable(now.getMonthOfYear(), 0));
		}
		if(!variables.containsKey("DAY")) {
			variables.put("DAY", new PathVariable(now.getDayOfMonth(), 0));
		}
		if(!variables.containsKey("HOUR")) {
			variables.put("HOUR", new PathVariable(now.getHourOfDay(), 0));
		}
	}
	
	private Path getPathFromPattern(String pattern) {
		StringBuffer replaced = new StringBuffer();
		String value = pattern;
		Matcher matcher = VARIABLE_PATTERN.matcher(value);
		while (matcher.find()) {
			String variableName = matcher.group(1);

			String replacement = String.valueOf(variables.get(variableName).getValue());
			
			matcher.appendReplacement(replaced, replacement);
			matcher.appendTail(replaced);

			value = replaced.toString();
			replaced = new StringBuffer();
			matcher = VARIABLE_PATTERN.matcher(value);
		}
		matcher.appendTail(replaced);
		
		Path p = new Path(fsName + replaced.toString());
		
		return p;
	}
	
	public String getDataSource() {
		return dataSource;
	}
	
	public static synchronized void init(Props props) {
		
		if(!init) {
			props.put("fs.hdfs.impl.disable.cache", "true");
			
			dataSource = props.getString("data.source");
			type = props.getString("checker.type");
			
			try {
				hadoopSecurityManager = loadHadoopSecurityManager(props, logger);
			}
			catch(RuntimeException e) {
				e.printStackTrace();
				throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
			}
	
			dataCheckingThread = new HdfsDataCheckingThread(props);
			dataCheckingThread.start();
			
			init = true;
			logger.info("HdfsDataChecker initiated");
		}
	}
	
	private static HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger logger) throws RuntimeException {

		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true, HdfsDataChecker.class.getClassLoader());
		logger.info("Initializing hadoop security manager " + hadoopSecurityManagerClass.getName());
		HadoopSecurityManager hadoopSecurityManager = null;

		try {
			Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
			hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(hadoopSecurityManagerClass, props);
		} 
		catch (InvocationTargetException e) {
			logger.error("Could not instantiate Hadoop Security Manager "+ hadoopSecurityManagerClass.getName() + e.getCause());
			throw new RuntimeException(e.getCause());
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getCause());
		}

		return hadoopSecurityManager;
	}
	
	@Override
	public String getType() {
		return type;
	}

	@Override
	public Boolean eval() {
		logger.info("Checking " + dataPaths.size() + " paths.");
		for(Path p : dataPaths) {
			dataCheckingThread.addDataToCheck(new Pair<Path, String>(p, hdfsUser));
			if(!dataCheckingThread.existRecord(new Pair<Path, String>(p, hdfsUser))) {
				return Boolean.FALSE;
			}
		}
		return Boolean.TRUE;
	}

	@Override
	public Object getNum() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		DateTime time = DateTime.now();
		for(String k : variables.keySet()) {
			PathVariable v = variables.get(k);
			if(v.equals("YEAR")) {
				time.withYear(v.getValue()).plusYears(v.getIncrement());
			} else if(v.equals("MONTH")) {
				time.withMonthOfYear(v.getValue()).plusMonths(v.getIncrement());
			} else if(v.equals("DAY")) {
				time.withDayOfMonth(v.getValue()).plusDays(v.getIncrement());
			} else if(v.equals("HOUR")) {
				time.withHourOfDay(v.getValue()).plusHours(v.getIncrement());
			} else {
				v.increment();
			}
		}
		
		variables.get("YEAR").setValue(time.getYear());
		variables.get("MONTH").setValue(time.getMonthOfYear());
		variables.get("DAY").setValue(time.getDayOfMonth());
		variables.get("HOUR").setValue(time.getHourOfDay());
		
		dataPaths = new ArrayList<Path>();
		for(String pattern : dataPathPatterns) {
			dataPaths.add(getPathFromPattern(pattern));
		}
	}
	
	public List<String> getDataPathPatterns() {
		return dataPathPatterns;
	}
	
	public Map<String, PathVariable> getVariables() {
		return variables;
	}
	
	public List<Path> getDataPaths() {
		return dataPaths;
	}
	
	@Override
	public String getId() {
		return checkerId;
	}

	public static ConditionChecker createFromJson(HashMap<String, Object> obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new RuntimeException("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		String checkerId = (String) jsonObj.get("checkerId");
		List<String> dataPathPatterns = (List<String>) jsonObj.get("dataPathPatterns");
		String hdfsUser = (String) jsonObj.get("hdfsUser");
		
		Map<String, PathVariable> variables = new HashMap<String, PathVariable>();
		Map<String, Object> vs = (HashMap<String, Object>) jsonObj.get("variables");
		for(String k : vs.keySet()) {
			List<String> vpair = (ArrayList<String>) vs.get(k);
			variables.put(k, new PathVariable(Integer.valueOf(vpair.get(0)), Integer.valueOf(vpair.get(1))));
		}
		HdfsDataChecker checker = new HdfsDataChecker(checkerId, hdfsUser, dataPathPatterns, variables);
		return checker;
	}
	
	@SuppressWarnings("unchecked")
	public static ConditionChecker createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new RuntimeException("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		String checkerId = (String) jsonObj.get("checkerId");
		List<String> dataPathPatterns = (List<String>) jsonObj.get("dataPathPatterns");
		String hdfsUser = (String) jsonObj.get("hdfsUser");
		
		Map<String, PathVariable> variables = new HashMap<String, PathVariable>();
		Map<String, Object> vs = (HashMap<String, Object>) jsonObj.get("variables");
		for(String k : vs.keySet()) {
			List<String> vpair = (ArrayList<String>) vs.get(k);
			variables.put(k, new PathVariable(Integer.valueOf(vpair.get(0)), Integer.valueOf(vpair.get(1))));
		}
		HdfsDataChecker checker = new HdfsDataChecker(checkerId, hdfsUser, dataPathPatterns, variables);
		return checker;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public ConditionChecker fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("checkerId", getId());
		jsonObj.put("dataPathPatterns", dataPathPatterns);
		jsonObj.put("hdfsUser", hdfsUser);
		Map<String, Object> vs = new HashMap<String, Object>();
		for(String k : variables.keySet()) {
			PathVariable v = variables.get(k);
			List<String> vpair = new ArrayList<String>();
			vpair.add(String.valueOf(v.getValue()));
			vpair.add(String.valueOf(v.getIncrement()));
			vs.put(k, vpair);
		}
		jsonObj.put("variables", vs);
		
		return jsonObj;
	}
	
	private static class HdfsDataCheckingThread extends Thread {
		
		private static final int MAX_NUM_DATA_RECORDS = 10000;
		private static final long RECORD_TIME_TO_LIVE = 7*24*60*60*1000;
		private Cache cache;
		private static final int DATA_CHECK_INTERVAL = 10000;
		private static final int MAX_DATA_CHECK_QUEUE_LENGTH = 1000;
		private boolean shutdown = false;
		private BlockingQueue<Pair<Path, String>> dataCheckQueue; 
		
		private long lastRunnerCheckTime = -1;
		
		public HdfsDataCheckingThread(Props props) {
			CacheManager manager = CacheManager.getInstance();
			
			cache = manager.createCache();
			cache.setEjectionPolicy(EjectionPolicy.LRU);
			cache.setMaxCacheSize(props.getInt("max.num.data.records", MAX_NUM_DATA_RECORDS));
			cache.setExpiryTimeToLiveMs(props.getLong("record.time.to.live", RECORD_TIME_TO_LIVE));
			
			int dataCheckQueueLength = props.getInt("data.check.queue.length", MAX_DATA_CHECK_QUEUE_LENGTH);
			//dataCheckQueue = new LinkedBlockingQueue<Pair<Path, String>>(dataCheckQueueLength);
			dataCheckQueue = new PriorityBlockingQueue<Pair<Path,String>>(dataCheckQueueLength, new DataCheckComparator());
		}
		
		private class DataCheckComparator implements Comparator<Pair<Path, String>> {
			@Override
			public int compare(Pair<Path, String> p1, Pair<Path, String> p2) {
				return (p1.getSecond()+p1.getFirst()).compareTo(p2.getSecond()+p2.getFirst());
			}
		}
		
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}
		
		public void run() {
			while(!shutdown) {
				synchronized (this) {
					try {
						lastRunnerCheckTime = System.currentTimeMillis();
						
						Pair<Path, String> p = dataCheckQueue.peek();
						if(p != null) {
							logger.info("Checking " + dataCheckQueue.size() + " paths.");
							for(Pair<Path, String> dataToCheck : dataCheckQueue) {
								logger.info("Checking " + dataToCheck.getFirst());
								if(checkHDFSData(dataToCheck)) {
									addRecord(dataToCheck);
									dataCheckQueue.remove(dataToCheck);
								}
							}
						}
						
						wait(DATA_CHECK_INTERVAL);
					}
					catch (Exception e) {
						logger.error("Error checking data from HDFS" + e);
					}
				}
			}
		}
		
		public synchronized void addDataToCheck(Pair<Path, String> data) {
			if(!(existRecord(data) || dataCheckQueue.contains(data))) {
				dataCheckQueue.add(data);
			}
		}
		
		private boolean checkHDFSData(Pair<Path, String> dataToCheck) throws HadoopSecurityManagerException, IOException {
			FileSystem fs = hadoopSecurityManager.getFSAsUser(dataToCheck.getSecond());
			return fs.exists(dataToCheck.getFirst());
		}

		private void addRecord(Pair<Path, String> dataCheck) {
			cache.put(dataCheck, true);
		}
		
		private void removeRecord(Pair<Path, String> dataCheck) {
			cache.remove(dataCheck);
		}
		
		public boolean existRecord(Pair<Path, String> dataCheck) {
			return cache.get(dataCheck) != null;
		}
		
	}

}
