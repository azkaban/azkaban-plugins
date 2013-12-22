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

package azkaban.viewer.pigvisualizer;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.jobtype.JobDagNode;
import azkaban.jobtype.MapReduceJobState;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.JSONUtils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.session.Session;

public class PigVisualizerServlet extends LoginAbstractAzkabanServlet {
	private static final String PROXY_USER_SESSION_KEY = 
			"hdfs.browser.proxy.user";
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = 
			"hadoop.security.manager.class";
	private static Logger logger = Logger.getLogger(PigVisualizerServlet.class);

	private Props props;
	private File webResourcesPath;

	private String viewerName;
	private String viewerPath;

	private ExecutorManagerAdapter executorManager;
	private ProjectManager projectManager;

	private String outputDir;

	public PigVisualizerServlet(Props props) {
		this.props = props;
		viewerName = props.getString("viewer.name");
		viewerPath = props.getString("viewer.path");

		webResourcesPath = new File(new File(props.getSource()).getParentFile().getParentFile(), "web");
		webResourcesPath.mkdirs();
		setResourceDirectory(webResourcesPath);

		outputDir = props.getString("pig.listener.output.dir",
				System.getProperty("java.io.tmpdir"));
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		AzkabanWebServer server = (AzkabanWebServer) getApplication();
		executorManager = server.getExecutorManager();
		projectManager = server.getProjectManager();

	}

  private void handleAllExecutions(HttpServletRequest request,
      HttpServletResponse response, Session session)
      throws ServletException, IOException {

		Page page = newPage(request, response, session, 
				"azkaban/viewer/pigvisualizer/pigvisualizer.vm");
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);
    page.add("errorMsg", "No job execution specified.");
		page.render();
  }

	private Project getProjectByPermission(int projectId, User user,
			Permission.Type type) {
		Project project = projectManager.getProject(projectId);
		if (project == null) {
			return null;
		}
		if (!hasPermission(project, user, type)) {
			return null;
		}
		return project;
	}

  private void handleVisualizer(HttpServletRequest request,
      HttpServletResponse response, Session session)
      throws ServletException, IOException {
		Page page = newPage(request, response, session, 
				"azkaban/viewer/pigvisualizer/pigvisualizer.vm");
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);

		int execId = Integer.parseInt(getParam(request, "execid"));
		String jobId = getParam(request, "jobid");
	
		ExecutableFlow exFlow = null;
		Project project = null;
		try {
			exFlow = executorManager.getExecutableFlow(execId);
			project = getProjectByPermission(
					exFlow.getProjectId(), session.getUser(), Type.READ);
		}
		catch (ExecutorManagerException e) {
			page.add("errorMsg", "Error getting project '" + e.getMessage() + "'");
			page.render();
			return;
		}

		if (project == null) {
			page.add("errorMsg", "Error getting project " + exFlow.getProjectId());
			page.render();
			return;
		}
		page.add("projectName", project.getName());
		page.add("execId", execId);
		page.add("jobId", jobId);
		page.add("flowId", exFlow.getFlowId());
		page.render();
	}

	private Map<String, JobDagNode> getDagNodeMap(int execId, String jobId) 
			throws Exception {
		String dagFilePath = outputDir + "/" + execId + "-" + jobId + 
				"-dagnodemap.json";

		File dagFile = new File(dagFilePath);
		Map<String, Object> jsonObj = (HashMap<String, Object>) 
			  JSONUtils.parseJSONFromFile(dagFile);
		Map<String, JobDagNode> dagNodeMap = new HashMap<String, JobDagNode>();
		for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
			dagNodeMap.put(entry.getKey(), JobDagNode.fromJson(entry.getValue()));
		}
		return dagNodeMap;
	}

	private Map<String, String> getNameToJobIdMap(
			Map<String, JobDagNode> dagNodeMap) {
		Map<String, String> nameToJobId = new HashMap<String, String>();
		for (Map.Entry<String, JobDagNode> entry : dagNodeMap.entrySet()) {
			JobDagNode node = entry.getValue();
			nameToJobId.put(node.getName(), node.getJobId());
		}
		return nameToJobId;
	}

	private void ajaxFetchJobDag(HttpServletRequest request,
			HttpServletResponse response, HashMap<String, Object> ret, User user,
			ExecutableFlow exFlow) throws ServletException {
		int execId = getIntParam(request, "execid");
		String jobId = getParam(request, "jobid");
		Map<String, JobDagNode> dagNodeMap = null;
		try {
			dagNodeMap = getDagNodeMap(execId, jobId);
		}
		catch (Exception e) {
			ret.put("error", "Error parsing JSON file: " + e.getMessage());
			return;
		}

		Map<String, String> nameToJobId = getNameToJobIdMap(dagNodeMap);
		ArrayList<Map<String, Object>> nodeList = new ArrayList<Map<String, Object>>();
		ArrayList<Map<String, Object>> edgeList = new ArrayList<Map<String, Object>>();
		for (Map.Entry<String, JobDagNode> entry : dagNodeMap.entrySet()) {
			JobDagNode node = entry.getValue();
			HashMap<String, Object> nodeObj = new HashMap<String, Object>();
			nodeObj.put("id", node.getJobId());
			nodeObj.put("level", node.getLevel());
			nodeObj.put("type", "pig");
			nodeList.add(nodeObj);

			// Add edges.
			for (String successor : node.getSuccessors()) {
				HashMap<String, Object> edgeObj = new HashMap<String, Object>();
				String successorJobId = nameToJobId.get(successor);
				if (successorJobId == null) {
					ret.put("error", "Node " + successor + " not found.");
					return;
				}

				JobDagNode targetNode = dagNodeMap.get(successorJobId);
				edgeObj.put("from", node.getJobId());
				edgeObj.put("target", targetNode.getJobId());
				edgeList.add(edgeObj);
			}
		}

		ret.put("nodes", nodeList);
		ret.put("edges", edgeList);
	}

	private void ajaxFetchJobDetails(HttpServletRequest request,
			HttpServletResponse response, HashMap<String, Object> ret, User user,
			ExecutableFlow exFlow) throws ServletException {
		int execId = getIntParam(request, "execid");
		String jobId = getParam(request, "jobid");
		String nodeId = getParam(request, "nodeid");
		Map<String, JobDagNode> dagNodeMap = null;
		try {
			dagNodeMap = getDagNodeMap(execId, jobId);
		}
		catch (Exception e) {
			ret.put("error", "Error parsing JSON file: " + e.getMessage());
			return;
		}
	
		JobDagNode node = dagNodeMap.get(nodeId);
		if (node == null) {
			ret.put("error", "Node " + nodeId + " not found.");
			return;
		}

		ret.put("jobId", nodeId);
		ret.put("stats", node.getJobStats().toJson());
		ret.put("features", node.getFeatures());
		ret.put("aliases", node.getAliases());
		ret.put("state", node.getMapReduceJobState().toJson());
	}
	
	private void handleAjaxAction(HttpServletRequest request,
			HttpServletResponse response, Session session) 
			throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(request, "ajax");
		if (hasParam(request, "execid")) {
			int execId = getIntParam(request, "execid");
			ExecutableFlow exFlow = null;
			try {
				exFlow = executorManager.getExecutableFlow(execId);
			}
			catch (ExecutorManagerException e) {
				ret.put("error", "Error fetching execution '" + execId + "': " +
						e.getMessage());
			}

			if (exFlow == null) {
				ret.put("error", "Cannot find execution '" + execId + "'");
			}
			else {
				if (ajaxName.equals("fetchjobdag")) {
					ajaxFetchJobDag(request, response, ret, session.getUser(), exFlow);
				}
				else if (ajaxName.equals("fetchjobdetails")) {
					ajaxFetchJobDetails(request, response, ret, session.getUser(), exFlow);
				}
			}
		}

		if (ret != null) {
			this.writeJSON(response, ret);
		}
	}
		
  @Override
	protected void handleGet(HttpServletRequest request, 
			HttpServletResponse response, Session session)
			throws ServletException, IOException {
		if (hasParam(request, "ajax")) {
			handleAjaxAction(request, response, session);
		}
		else if (hasParam(request, "execid") && hasParam(request, "jobid")) {
      handleVisualizer(request, response, session);
		}
		else {
      handleAllExecutions(request, response, session);
    }
	}

	@Override
	protected void handlePost(HttpServletRequest request,
			HttpServletResponse response, Session session)
			throws ServletException, IOException {
		if (hasParam(request, "ajax")) {
			handleAjaxAction(request, response, session);
		}
	}
}
