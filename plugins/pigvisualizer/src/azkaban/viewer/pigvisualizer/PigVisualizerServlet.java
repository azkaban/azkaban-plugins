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

	private String viewerName;
	private String viewerPath;

	private ExecutorManagerAdapter executorManager;
	private ProjectManager projectManager;

	public PigVisualizerServlet(Props props) {
		this.props = props;
		viewerName = props.getString("viewer.name");
		viewerPath = props.getString("viewer.path");
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
				"azkaban/viewer/pigvisualizer/allexecutions.vm");
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);

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

	private Map<String, JobDagNode> getDagNodeJobNameMap(String jsonDir) 
			throws Exception {
		String outputDagNodeNameFile = jsonDir + "-dagnodemap.json";
		File dagNodeNameMapFile = new File(outputDagNodeNameFile);
		Map<String, Object> jsonObj = (HashMap<String, Object>) 
			  JSONUtils.parseJSONFromFile(dagNodeNameMapFile);
		Map<String, JobDagNode> dagNodeJobNameMap =
				new HashMap<String, JobDagNode>();
		for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
			dagNodeJobNameMap.put(entry.getKey(),
					JobDagNode.fromJson(entry.getValue()));
		}
		return dagNodeJobNameMap;
	}

	private Map<String, JobDagNode> getDagNodeJobIdMap(String jsonDir) 
			throws Exception {
		String outputDagNodeJobIdFile = jsonDir + "-dagnodejobidmap.json";
		File dagNodeJobIdMapFile = new File(outputDagNodeJobIdFile);
		Map<String, Object> jsonObj = (HashMap<String, Object>)
				JSONUtils.parseJSONFromFile(dagNodeJobIdMapFile);
		Map<String, JobDagNode> dagNodeJobIdMap = 
			new HashMap<String, JobDagNode>(); 
		for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
			dagNodeJobIdMap.put(entry.getKey(), 
					JobDagNode.fromJson(entry.getValue()));
		}
		return dagNodeJobIdMap;
	}

	private List<String> getCompletedJobIds(String jsonDir) 
			throws Exception {
		String outputCompletedJobIdsFile = jsonDir + "-completedjobs.json";
		File completedJobIdsFile = new File(outputCompletedJobIdsFile);
		return (ArrayList<String>) JSONUtils.parseJSONFromFile(completedJobIdsFile);
	}

  private void handleVisualizer(HttpServletRequest request,
      HttpServletResponse response, Session session, String path)
      throws ServletException, IOException {

		Page page = newPage(request, response, session, 
				"azkaban/viewer/pigvisualizer/visualizer.vm");
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);

    String[] parts = path.split("/");
    if (!parts[1].equals("execution") || parts.length != 4) {
      page.add("errorMsg", "Invalid parameters.");
      page.render();
      return;
    }

    int execId = Integer.parseInt(parts[2]);
    String jobId = parts[3];
		ExecutableFlow exFlow = null;
	  try {
			exFlow = executorManager.getExecutableFlow(execId);
		}
		catch (ExecutorManagerException e) {
			page.add("errorMsg", "Error fetching execution '" + execId + "': " + 
					e.getMessage());
			page.render();
			return;
		}
		
		User user = session.getUser();
		Project project = getProjectByPermission(
				exFlow.getProjectId(), user, Type.READ);
		if (project == null) {
			page.add("errorMsg", "Error getting project " + exFlow.getProjectId());
			page.render();
			return;
		}

		// TODO: Refactor getting the json files etc.

		String jsonDir = "./executions/" + execId + "/" + jobId;
		Map<String, JobDagNode> dagNodeNameMap = null;
		Map<String, JobDagNode> dagNodeJobIdMap = null;
		List<String> completedJobIds = null;
		try {
			dagNodeNameMap = getDagNodeJobNameMap(jsonDir);
			dagNodeJobIdMap = getDagNodeJobIdMap(jsonDir);
			completedJobIds = getCompletedJobIds(jsonDir);
		}
		catch (Exception e) {
			page.add("errorMsag", "Error parsing JSON files " + e.getMessage());
			page.render();
			return;
		}

    page.add("execid", execId);
    page.add("job", jobId);
		page.render();
  }
	
  @Override
	protected void handleGet(HttpServletRequest request, 
			HttpServletResponse response, Session session)
			throws ServletException, IOException {
		User user = session.getUser();
		String username = user.getUserId();

    String prefix = request.getContextPath() + request.getServletPath();
    String path = request.getRequestURI().substring(prefix.length());

    if (path.length() == 0) {
      handleAllExecutions(request, response, session);
    } else {
      handleVisualizer(request, response, session, path);
    }
	}

	@Override
	protected void handlePost(HttpServletRequest request,
			HttpServletResponse response, Session session)
			throws ServletException, IOException {

	}
}
