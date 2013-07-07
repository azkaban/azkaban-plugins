package azkaban.triggertype.HdfsDataTrigger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.trigger.TriggerAction;
import azkaban.triggertype.HdfsDataTrigger.HdfsDataChecker.PathVariable;
import azkaban.utils.Pair;

public class HdfsDataTrigger {
	
	private int id = -1;
	private String dataSource;
	private List<String> dependentDataPatterns;
	private String hdfsUser;
	private Map<String, PathVariable> variables;
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
			String hdfsUser,
			Map<String, PathVariable> variables, 
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
		this.hdfsUser = hdfsUser;
		this.variables = variables;
		this.timeToExpire = timeToExpire;
		this.actions = actions;
		this.projectId = projectId;
		this.flowName = flowName;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
	}
	
	public String getHdfsUser() {
		return hdfsUser;
	}

	public void setHdfsUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
	}

	public HdfsDataTrigger(
			String dataSource, 
			List<String> dataPatterns, 
			String hdfsUser,
			Map<String, PathVariable> variables, 
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
		this.hdfsUser = hdfsUser;
		this.variables = variables;
		this.timeToExpire = timeToExpire;
		this.actions = actions;
		this.projectId = projectId;
		this.flowName = flowName;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
	}
	
	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public List<String> getDependentDataPatterns() {
		return dependentDataPatterns;
	}

	public void setDependentDataPatterns(List<String> dependentDataPatterns) {
		this.dependentDataPatterns = dependentDataPatterns;
	}

	public Map<String, PathVariable> getVariables() {
		return variables;
	}

	public void setVariables(Map<String, PathVariable> variables) {
		this.variables = variables;
	}

	public int getProjectId() {
		return projectId;
	}

	public void setProjectId(int projectId) {
		this.projectId = projectId;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public String getFlowName() {
		return flowName;
	}

	public void setTimeToExpire(ReadablePeriod timeToExpire) {
		this.timeToExpire = timeToExpire;
	}

	public void setLastModifyTime(DateTime lastModifyTime) {
		this.lastModifyTime = lastModifyTime;
	}

	public void setSubmitTime(DateTime submitTime) {
		this.submitTime = submitTime;
	}

	public void setSubmitUser(String submitUser) {
		this.submitUser = submitUser;
	}

	public DateTime getLastModifyTime() {
		return lastModifyTime;
	}
	
	public DateTime getSubmitTime() {
		return submitTime;
	}
	
	public Pair<Integer, String> getIdPair() {
		return new Pair<Integer, String>(projectId, flowName);
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

	public String getDescription() {
		return "Hdfs Data Trigger " + getId();
	}

}
