package azkaban.triggertype.HdfsDataTrigger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutorManager;
import azkaban.project.ProjectManager;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerServicer;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public class HdfsDataTriggerManager implements TriggerServicer{
	
	private static final Logger logger = Logger.getLogger(HdfsDataTriggerManager.class);
	
	private HdfsDataTriggerLoader loader;
	
	private Map<Pair<Integer, String>, HdfsDataTrigger> dataTriggers;
	
	public HdfsDataTriggerManager(Props props, TriggerManager triggerManager, ExecutorManager executorManager, ProjectManager projectManager) {
		loader = new HdfsDataTriggerLoader(props, triggerManager, executorManager, projectManager);
	}
	
	@Override
	public void load() {
		logger.info("Hdfs data trigger manager loading up");
		List<HdfsDataTrigger> dts = loader.loadDataTriggers();
		dataTriggers = new HashMap<Pair<Integer,String>, HdfsDataTrigger>();
		for(HdfsDataTrigger dt : dts) {
			dataTriggers.put(dt.getIdPair(), dt);
		}
	}
	
	public List<HdfsDataTrigger> getDataTriggers() {
		return new ArrayList<HdfsDataTrigger>(dataTriggers.values());
	}
	
	public void addDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.insertDataTrigger(dt);
		dataTriggers.put(dt.getIdPair(), dt);
	}
	
	public void deleteDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.removeDataTrigger(dt);
		dataTriggers.remove(dt.getIdPair());
	}
	
	public void updateDataTrigger(HdfsDataTrigger dt) throws Exception {
		loader.updateDataTrigger(dt);
		dataTriggers.put(dt.getIdPair(), dt);
	}

	@Override
	public void createTriggerFromFile(int projectId, String flowName, File triggerFile) {
		// TODO Auto-generated method stub
		
	}
	
}
