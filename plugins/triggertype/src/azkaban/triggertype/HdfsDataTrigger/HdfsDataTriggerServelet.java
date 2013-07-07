package azkaban.triggertype.HdfsDataTrigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.actions.ExecuteFlowAction;
import azkaban.executor.ExecutionOptions;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.TriggerAction;
import azkaban.triggertype.HdfsDataTrigger.HdfsDataChecker.PathVariable;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.servlet.HttpRequestUtils;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.session.Session;

public class HdfsDataTriggerServelet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = Logger.getLogger(HdfsDataTriggerServelet.class);
	private HdfsDataTriggerManager hdfsDataTriggerManager;
	private ProjectManager projectManager;

	private String webPath;
	private String pageTitle;
	
	public HdfsDataTriggerServelet(Props props, HdfsDataTriggerManager hdfsDataTriggerManager, ProjectManager projectManager) {
		this.hdfsDataTriggerManager = hdfsDataTriggerManager;
		this.projectManager = projectManager;
		this.webPath = props.getString("trigger.web.path");
		this.pageTitle = props.getString("trigger.page.title");
	}
	
	public String getTriggerSource() {
		return hdfsDataTriggerManager.getTriggerSource();
	}
	
	public String getWebPath() {
		return webPath;
	}
	
	public String getPageTitle() {
		return pageTitle;
	}
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		} else {
			handleGetAllHdfsDataTriggers(req, resp, session);
		}
	}
	
	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(req, "ajax");
		
		try {
			if (ajaxName.equals("removeTrigger")) {
				ajaxRemoveTrigger(req, ret, session.getUser());
			} else if (ajaxName.equals("setTrigger")) {
				ajaxSetTrigger(req, ret, session.getUser());
			} else if (ajaxName.equals("fetchConfig")) {
				ajaxFetchConfig(req, ret, session.getUser());
			}
			else {
				throw new Exception("Action " + ajaxName + " not supported!");
			}
		} catch (Exception e) {
			ret.put("error", e.getMessage());
		}
		
		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}



	private void handleGetAllHdfsDataTriggers(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException{
		
		Page page = newPage(req, resp, session, "azkaban/triggertype/HdfsDataTrigger/hdfsdatatriggerdisplaypage.vm");
		page.add("triggerSource", hdfsDataTriggerManager.getTriggerSource());
		page.add("webPath", webPath);
		page.add("pageTitle", pageTitle);
		
		List<HdfsDataTrigger> triggers = hdfsDataTriggerManager.getDataTriggers();
		page.add("triggers", triggers);
//		
//		List<SLA> slas = slaManager.getSLAs();
//		page.add("slas", slas);

		page.render();
	}
	
	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
	}

	

	private void ajaxFetchConfig(HttpServletRequest req,
			HashMap<String, Object> ret, User user) {
		ret.put("dataSource", hdfsDataTriggerManager.getTriggerSource());
		ret.put("configMessage", "This is Data Trigger for LocalHdfs");
		ret.put("hdfsUser", user.getUserId());
	}
	
	private void ajaxSetTrigger(HttpServletRequest req, HashMap<String, Object> ret, User user) throws Exception {
		String projectName = getParam(req, "projectName");
		String flowName = getParam(req, "flow");
		int projectId = getIntParam(req, "projectId");
		
		Project project = projectManager.getProject(projectId);
			
		if (project == null) {
			ret.put("message", "Project " + projectName + " does not exist");
			ret.put("status", "error");
			return;
		}
		
//		if (!hasPermission(project, user, Type.SCHEDULE)) {
//			ret.put("status", "error");
//			ret.put("message", "Permission denied. Cannot execute " + flowName);
//			return;
//		}
		
		String dataSource = getParam(req, "datasource", hdfsDataTriggerManager.getTriggerSource());
		String hdfsUser = getParam(req, "hdfsuser", user.getUserId());
		String expireTimeString = getParam(req, "timetoexpire", "24h");
		ReadablePeriod timeToExpire = Utils.parsePeriodString(expireTimeString);
		
		Map<String, String> patterns = getParamGroup(req, "patterns");
		List<String> dataPatterns;
		if (!patterns.isEmpty()) {
			//String[] patternsSplit = patterns.split("\\s*,\\s*|\\s*;\\s*|\\s+");
			//dataPatterns = Arrays.asList(patternsSplit);
			dataPatterns = new ArrayList<String>(patterns.values());
		} else {
			throw new ServletException("Data pattern missing!");
		}
		
		Map<String, String> varsRaw = getParamGroup(req, "variables");
		Map<String, PathVariable> variables = new HashMap<String, HdfsDataChecker.PathVariable>();
		if(!varsRaw.isEmpty()) {
			for(String vs : varsRaw.keySet()) {
				String[] vparts = varsRaw.get(vs).split(",");
				int start = Integer.valueOf(vparts[0]);
				int inc = Integer.valueOf(vparts[1]);
				variables.put(vs, new PathVariable(start, inc));
			}
		}
		
		ExecutionOptions flowOptions = null;
		try {
			flowOptions = HttpRequestUtils.parseFlowOptions(req);
		}
		catch (Exception e) {
			ret.put("error", e.getMessage());
		}
		
		List<TriggerAction> actions = new ArrayList<TriggerAction>();
		TriggerAction act = new ExecuteFlowAction(projectId, projectName, flowName, user.getUserId(), flowOptions);
		actions.add(act);
		DateTime now = DateTime.now();
		HdfsDataTrigger dt = new HdfsDataTrigger(dataSource, dataPatterns, hdfsUser, variables, timeToExpire, projectId, flowName, actions, now, now, user.getUserId());
		
		hdfsDataTriggerManager.addDataTrigger(dt);
		
		logger.info("User '" + user.getUserId() + "' has set trigger for " + "[" + projectName + flowName +  " (" + projectId +")" + "].");
//		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + schedule.toString() + " has been added.");

		ret.put("status", "success");
		ret.put("message", projectName + "." + flowName + " trigger set.");

	}
	
	private void ajaxRemoveTrigger(HttpServletRequest req, Map<String, Object> ret, User user) throws Exception{
		int triggerId = getIntParam(req, "triggerId");
		HdfsDataTrigger t = hdfsDataTriggerManager.getDataTrigger(triggerId);
		if(t == null) {
			ret.put("message", "Trigger with ID " + triggerId + " does not exist");
			ret.put("status", "error");
			return;
		}
		
//		if(!hasPermission(project, user, Type.SCHEDULE)) {
//			ret.put("status", "error");
//			ret.put("message", "Permission denied. Cannot remove trigger with id " + triggerId);
//			return;
//		}

		hdfsDataTriggerManager.deleteDataTrigger(t);
		logger.info("User '" + user.getUserId() + " has removed hdfs data trigger " + t.getDescription());
//		projectManager.postProjectEvent(project, EventType.SCHEDULE, user.getUserId(), "Schedule " + sched.toString() + " has been removed.");
		
		ret.put("status", "success");
		ret.put("message", "Hdfs Data Trigger " + triggerId + " removed from Triggers.");
		return;
	}

	public String getInputPanelVM() {
		return "azkaban/triggertype/HdfsDataTrigger/hdfsdatatriggeroptionspanel.vm";
	}


}
