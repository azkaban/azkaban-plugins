package azkaban.triggertype.HdfsDataTrigger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.trigger.TriggerAction;
import azkaban.utils.Pair;

public class HdfsDataTrigger {
	
	private int id = -1;
	private String dataSource;
	private List<String> dependentDataPatterns;
	private Map<String, Pair<Integer, Integer>> variables;
	private ReadablePeriod timeToExpire;
	
	private List<TriggerAction> actions; 
	private int projectId;
	private String flowName;
	
	private DateTime lastModifyTime;
	private DateTime submitTime;
	private String submitUser;
	
	public HdfsDataTrigger(
			int id, 
			String dataSource, 
			List<String> dataPatterns, 
			Map<String, Pair<Integer, Integer>> variables, 
			ReadablePeriod timeToExpire, 
			int projectId,
			String flowName,
			List<TriggerAction> actions,
			DateTime lastModifyTime,
			DateTime submitTime,
			String submitUser
			) {
		this.id = id;
		this.dataSource = dataSource;
		this.dependentDataPatterns = dataPatterns;
		this.variables = variables;
		this.timeToExpire = timeToExpire;
		this.actions = actions;
		this.projectId = projectId;
		this.flowName = flowName;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
	}
	
	public HdfsDataTrigger(
			String dataSource, 
			List<String> dataPatterns, 
			Map<String, Pair<Integer, Integer>> variables, 
			ReadablePeriod timeToExpire, 
			int projectId,
			String flowName,
			List<TriggerAction> actions,
			DateTime lastModifyTime,
			DateTime submitTime,
			String submitUser
			) {
		this.dataSource = dataSource;
		this.dependentDataPatterns = dataPatterns;
		this.variables = variables;
		this.timeToExpire = timeToExpire;
		this.actions = actions;
		this.projectId = projectId;
		this.flowName = flowName;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
	}
	
	public DateTime getLastModifyTime() {
		return lastModifyTime;
	}
	
	public DateTime getSubmitTime() {
		return submitTime;
	}
	
	public int getDataTriggerId() {
		return id;
	}
	
	public Pair<Integer, String> getIdPair() {
		return new Pair<Integer, String>(projectId, flowName);
	}
	
	public void setDataTriggerId(int id) {
		this.id = id;
	}
	
	public String getSubmitUser() {
		return submitUser;
	}
	
	public ReadablePeriod getTimeToExpire() {
		return timeToExpire;
	}
	
	public List<TriggerAction> getActions() {
		return actions;
	}
}
