package azkaban.triggertype.HdfsDataTrigger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.actions.ExecuteFlowAction;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManager;
import azkaban.project.ProjectManager;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerAgent;
import azkaban.trigger.TriggerStatus;
import azkaban.triggertype.HdfsDataTrigger.HdfsDataChecker.PathVariable;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class HdfsDataTriggerManager implements TriggerAgent{
	
	private static final Logger logger = Logger.getLogger(HdfsDataTriggerManager.class);
	
	private HdfsDataTriggerLoader loader;
	
	private Map<Pair<Integer, String>, HdfsDataTrigger> dataTriggers;
	private Map<Integer, HdfsDataTrigger> dataTriggerIdMap;
	
	private final String triggerSource;
	private final String dataSource;
	private final String defaultExpireTime;
	
	public HdfsDataTriggerManager(Props props, TriggerManager triggerManager, ExecutorManager executorManager, ProjectManager projectManager) {
		this.triggerSource = props.getString("trigger.source.name");
		this.dataSource = props.getString("data.source");
		this.defaultExpireTime = props.getString("default.time.to.expire", "24h");
		HdfsDataChecker.init(props);
		triggerManager.getCheckerLoader().registerCheckerType(HdfsDataChecker.type, HdfsDataChecker.class);
		loader = new HdfsDataTriggerLoader(props, triggerManager, executorManager, projectManager, triggerSource);
		
	}
	
	@Override
	public void load() {
		logger.info("Hdfs data trigger manager loading up");
		List<HdfsDataTrigger> dts = loader.loadDataTriggers();
		dataTriggers = new HashMap<Pair<Integer,String>, HdfsDataTrigger>();
		dataTriggerIdMap = new HashMap<Integer, HdfsDataTrigger>();
		for(HdfsDataTrigger dt : dts) {
			dataTriggers.put(dt.getIdPair(), dt);
			dataTriggerIdMap.put(dt.getId(), dt);
		}
	}
	
	public List<HdfsDataTrigger> getDataTriggers() {
		return new ArrayList<HdfsDataTrigger>(dataTriggers.values());
	}
	
	public void addDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.insertDataTrigger(dt);
		dataTriggers.put(dt.getIdPair(), dt);
		dataTriggerIdMap.put(dt.getId(), dt);
	}
	
	public void deleteDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.removeDataTrigger(dt);
		dataTriggers.remove(dt.getIdPair());
		dataTriggerIdMap.remove(dt.getId());
	}
	
	public HdfsDataTrigger getDataTrigger(int id) {
		return dataTriggerIdMap.get(id);
	}
	
	public void updateDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.updateDataTrigger(dt);
		dataTriggers.put(dt.getIdPair(), dt);
		dataTriggerIdMap.put(dt.getId(), dt);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void loadTriggerFromProps(Props props) throws Exception{
		String hdfsUser = props.getString("hdfsUser");
		List<String> dataPathPatterns = props.getStringList("dataPathPatterns");
		Map<String, String> variableMap = props.getMapByPrefix("variable.");
		Map<String, PathVariable> variables = new HashMap<String, HdfsDataChecker.PathVariable>();
		for(String k : variableMap.keySet()) {
			List<String> v =  Arrays.asList(variableMap.get(k).split(","));
			PathVariable p = new PathVariable(Integer.valueOf(v.get(0)), Integer.valueOf(v.get(1)));
			variables.put(k, p);
		}
		String expireTime = props.getString("time.to.expire", defaultExpireTime);
		ReadablePeriod timeToExpire = Utils.parsePeriodString(expireTime);
		
		int projectId = props.getInt("projectId");
		String flowName = props.getString("flowName");
		String projectName = props.getString("projectName");
		String submitUser = props.getString("submitUser");
		
		List<TriggerAction> actions = new ArrayList<TriggerAction>();
		TriggerAction action = new ExecuteFlowAction(projectId, projectName, flowName, submitUser, new ExecutionOptions());
		actions.add(action);
		DateTime now = DateTime.now();
		
		HdfsDataTrigger dt = new HdfsDataTrigger(dataSource, dataPathPatterns, hdfsUser, variables, timeToExpire, projectId, flowName, actions, now, now, submitUser);
		addDataTrigger(dt);
	}

	@Override
	public String getTriggerSource() {
		return triggerSource;
	}

	@Override
	public void updateLocal(Trigger t) {
		if(t.getStatus().equals(TriggerStatus.EXPIRED)) {
			try {
				deleteDataTrigger(getDataTrigger(t.getTriggerId()));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("failed to remove expired datatrigger " + t.getDescription());
			}
		}
		
	}

}
