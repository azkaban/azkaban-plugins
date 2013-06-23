package azkaban.triggertype.HdfsDataTrigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import azkaban.actions.ExecuteFlowAction;
import azkaban.executor.ExecutorManager;
import azkaban.project.ProjectManager;
import azkaban.scheduler.BasicTimeChecker;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;
import azkaban.utils.Props;

public class HdfsDataTriggerLoader {
	
	private static final Logger logger = Logger.getLogger(HdfsDataTriggerLoader.class);
	
//	private Map<String, Class <? extends HdfsDataChecker>> dataCheckerMap;
	
	private TriggerManager triggerManager;
	
	private final String triggerSource;
	
	@SuppressWarnings("unchecked")
	public HdfsDataTriggerLoader(Props props, TriggerManager triggerManager, ExecutorManager executorManager, ProjectManager projectManager, String triggerSource) {

		this.triggerSource = triggerSource;
		
		// prepare executeflow action
		if(ExecuteFlowAction.getExecutorManager() == null ) {
			ExecuteFlowAction.setExecutorManager(executorManager);
		}
		if(ExecuteFlowAction.getProjectManager() == null ) {
			ExecuteFlowAction.setProjectManager(projectManager);
		}
		
		this.triggerManager = triggerManager;
		
		// get all available data sources
//		List<String> dataSources = props.getStringList("azkaban.datatrigger.datasources");
//		Map<String, Class<? extends ConditionChecker>> allCheckers = triggerManager.getSupportedCheckers();
//		for(String ds : allCheckers.keySet()) {
//			if(ds.endsWith("DataChecker")) {
//				dataCheckerMap.put(ds, (Class<? extends HdfsDataChecker>) allCheckers.get(ds));
//			}
//		}
		
	}
	
	public List<HdfsDataTrigger> loadDataTriggers() {
		List<Trigger> triggers = triggerManager.getTriggers();
		List<HdfsDataTrigger> datatriggers = new ArrayList<HdfsDataTrigger>();
		for(Trigger t : triggers) {
			if(t.getSource().equals(triggerSource)) {
				datatriggers.add(triggerToDataTriger(t));
			}
		}
		logger.info("Loaded " + datatriggers.size() + " data triggers from " + triggerSource);
		return datatriggers;
	}
	
	public String getSource() {
		return triggerSource;
	}
	
	public HdfsDataTrigger triggerToDataTriger(Trigger t) {
		HdfsDataChecker checker = (HdfsDataChecker) t.getTriggerCondition().getCheckers().values().toArray()[0];
		BasicTimeChecker expireChecker = (BasicTimeChecker) t.getExpireCondition().getCheckers().values().toArray()[0];
		ExecuteFlowAction action = (ExecuteFlowAction) t.getActions().get(0);
		HdfsDataTrigger dt = new HdfsDataTrigger(
				checker.getDataSource(), 
				checker.getDataPathPatterns(),
				checker.getHdfsUser(),
				checker.getVariables(), 
				expireChecker.getPeriod(), 
				action.getProjectId(), 
				action.getFlowName(), 
				t.getActions(),
				t.getLastModifyTime(),
				t.getSubmitTime(),
				t.getSubmitUser());
		return dt;
	}
	
	public Trigger dataTriggerToTrigger(HdfsDataTrigger dt) throws Exception {
		Condition triggerCondition = createTriggerCondition(dt);
		Condition expireCondition = createExpireCondition(dt);
		Trigger t = new Trigger(
				dt.getDataTriggerId(), 
				dt.getLastModifyTime(), 
				dt.getSubmitTime(), 
				dt.getSubmitUser(), 
				triggerSource, 
				triggerCondition, 
				expireCondition, 
				dt.getActions());
		return t;
	}
	
	public void insertDataTrigger(HdfsDataTrigger dt) throws Exception {
		Trigger t = dataTriggerToTrigger(dt);
		try {
			triggerManager.insertTrigger(t);
			dt.setDataTriggerId(t.getTriggerId());
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e);
		}
	}
	
	private Condition createTriggerCondition(HdfsDataTrigger dt) throws Exception {
		HdfsDataChecker checker = createDataChecker(dt);
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		checkers.put(checker.getId(), checker);
		String expr = checker.getId() + ".eval()";
		Condition cond = new Condition(checkers, expr);
		return cond;
	}
	
	private static Condition createExpireCondition(HdfsDataTrigger dt) {
		BasicTimeChecker checker = new BasicTimeChecker("DataCheckExpireChecker", DateTime.now(), DateTimeZone.UTC, true, true, dt.getTimeToExpire());
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		checkers.put(checker.getId(), checker);
		String expr = checker.getId() + ".eval()";
		Condition cond = new Condition(checkers, expr);
		return cond;
	}
	
	private HdfsDataChecker createDataChecker(HdfsDataTrigger dt) throws Exception {
		HdfsDataChecker checker = new HdfsDataChecker(triggerSource+"DataChecker", dt.getHdfsUser(), dt.getDependentDataPatterns(), dt.getVariables());
		return checker;
	}
	
	public void updateDataTrigger(HdfsDataTrigger dt) throws Exception {
		Trigger t = dataTriggerToTrigger(dt);
		try {
			triggerManager.updateTrigger(t);
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e);
		}
	}
	
	public void removeDataTrigger(HdfsDataTrigger dt) throws Exception {
		try {
			triggerManager.removeTrigger(dt.getDataTriggerId());
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e);
		}
	}
}
