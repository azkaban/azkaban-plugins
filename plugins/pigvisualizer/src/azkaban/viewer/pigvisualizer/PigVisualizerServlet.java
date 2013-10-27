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

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.Reportal;
import azkaban.reportal.util.Reportal.Query;
import azkaban.reportal.util.Reportal.Variable;
import azkaban.reportal.util.ReportalHelper;
import azkaban.reportal.util.ReportalUtil;
import azkaban.reportal.util.StreamProviderHDFS;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleManagerException;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.session.Session;

public class PigVisualizerServlet extends LoginAbstractAzkabanServlet {
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(PigVisualizerServlet.class);

	private AzkabanWebServer server;
	private Props props;
	private boolean shouldProxy;

	private String viewerName;
	private File webResourcesFolder;

	// private String viewerPath;
	private HadoopSecurityManager hadoopSecurityManager;

	public PigVisualizerServlet(Props props) {
		this.props = props;

		viewerName = props.getString("viewer.name");
		webResourcesFolder = new File(new File(props.getSource()).getParentFile().getParentFile(), "web");
		webResourcesFolder.mkdirs();
		setResourceDirectory(webResourcesFolder);
		System.out.println("Visualizer web resources: " + webResourcesFolder.getAbsolutePath());
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		server = (AzkabanWebServer)getApplication();

		shouldProxy = props.getBoolean("azkaban.should.proxy", false);
		logger.info("Hdfs browser should proxy: " + shouldProxy);
		try {
			hadoopSecurityManager = loadHadoopSecurityManager(props, logger);
		} catch (RuntimeException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
		}
	}

	private HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger logger) throws RuntimeException {

		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true, PigVisualizerServlet.class.getClassLoader());
		logger.info("Initializing hadoop security manager " + hadoopSecurityManagerClass.getName());
		HadoopSecurityManager hadoopSecurityManager = null;

		try {
			Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
			hadoopSecurityManager = (HadoopSecurityManager)getInstanceMethod.invoke(hadoopSecurityManagerClass, props);
		} catch (InvocationTargetException e) {
			logger.error("Could not instantiate Hadoop Security Manager " + hadoopSecurityManagerClass.getName() + e.getCause());
			throw new RuntimeException(e.getCause());
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getCause());
		}

		return hadoopSecurityManager;
	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
		else {
			if (hasParam(req, "view")) {
				try {
					handleViewReportal(req, resp, session);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else if (hasParam(req, "new")) {
				handleNewReportal(req, resp, session);
			}
			else if (hasParam(req, "edit")) {
				handleEditReportal(req, resp, session);
			}
			else if (hasParam(req, "run")) {
				handleRunReportal(req, resp, session);
			}
			else {
				handleListReportal(req, resp, session);
			}
		}
	}

	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(req, "ajax");
		User user = session.getUser();
		int id = getIntParam(req, "id");
		ProjectManager projectManager = server.getProjectManager();
		Project project = projectManager.getProject(id);
		Reportal reportal = Reportal.loadFromProject(project);

		// Delete reportal
		if (ajaxName.equals("delete")) {
			if (!project.hasPermission(user, Type.ADMIN)) {
				ret.put("error", "You do not have permissions to delete this reportal.");
			}
			else {
				try {
					ScheduleManager scheduleManager = server.getScheduleManager();
					reportal.removeSchedules(scheduleManager);
					projectManager.removeProject(project, user);
				} catch (Exception e) {
					e.printStackTrace();
					ret.put("error", "An exception occured while deleting this reportal.");
				}
				ret.put("result", "success");
			}
		}
		// Bookmark reportal
		else if (ajaxName.equals("bookmark")) {
			boolean wasBookmarked = ReportalHelper.isBookmarkProject(project, user);
			try {
				if (wasBookmarked) {
					ReportalHelper.unBookmarkProject(server, project, user);
					ret.put("result", "success");
					ret.put("bookmark", false);
				}
				else {
					ReportalHelper.bookmarkProject(server, project, user);
					ret.put("result", "success");
					ret.put("bookmark", true);
				}
			} catch (ProjectManagerException e) {
				e.printStackTrace();
				ret.put("error", "Error bookmarking reportal. " + e.getMessage());
			}
		}
		// Subscribe reportal
		else if (ajaxName.equals("subscribe")) {
			boolean wasSubscribed = ReportalHelper.isSubscribeProject(project, user);
			if (!wasSubscribed && !project.hasPermission(user, Type.READ)) {
				ret.put("error", "You do not have permissions to view this reportal.");
			}
			else {
				try {
					if (wasSubscribed) {
						ReportalHelper.unSubscribeProject(server, project, user);
						ret.put("result", "success");
						ret.put("subscribe", false);
					}
					else {
						ReportalHelper.subscribeProject(server, project, user, user.getEmail());
						ret.put("result", "success");
						ret.put("subscribe", true);
					}
				} catch (ProjectManagerException e) {
					e.printStackTrace();
					ret.put("error", "Error subscribing to reportal. " + e.getMessage());
				}
			}
		}
		// Set graph
		else if (ajaxName.equals("graph")) {
			if (!project.hasPermission(user, Type.READ)) {
				ret.put("error", "You do not have permissions to view this reportal.");
			}
			else {
				String hash = getParam(req, "hash");
				project.getMetadata().put("graphHash", hash);
				try {
					server.getProjectManager().updateProjectSetting(project);
					ret.put("result", "success");
					ret.put("message", "Default graph saved.");
				} catch (ProjectManagerException e) {
					e.printStackTrace();
					ret.put("error", "Error saving graph. " + e.getMessage());
				}
			}
		}
		// Get a portion of logs
		else if (ajaxName.equals("log")) {
			int execId = getIntParam(req, "execId");
			String jobId = getParam(req, "jobId");
			int offset = getIntParam(req, "offset");
			int length = getIntParam(req, "length");
			ExecutableFlow exec;
			ExecutorManagerAdapter executorManager = server.getExecutorManager();
			try {
				exec = executorManager.getExecutableFlow(execId);
			} catch (Exception e) {
				ret.put("error", "Log does not exist or isn't created yet.");
				return;
			}

			LogData data;
			try {
				data = executorManager.getExecutionJobLog(exec, jobId, offset, length, exec.getExecutableNode(jobId).getAttempt());
			} catch (Exception e) {
				e.printStackTrace();
				ret.put("error", "Log does not exist or isn't created yet.");
				return;
			}
			if (data != null) {
				ret.put("result", "success");
				ret.put("log", data.getData());
				ret.put("offset", data.getOffset());
				ret.put("length", data.getLength());
				ret.put("completed", exec.getEndTime() != -1);
			}
			else {
				// Return an empty result to indicate the end
				ret.put("result", "success");
				ret.put("log", "");
				ret.put("offset", offset);
				ret.put("length", 0);
				ret.put("completed", exec.getEndTime() != -1);
			}
		}

		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			HashMap<String, Object> ret = new HashMap<String, Object>();

			handleRunReportalWithVariables(req, ret, session);

			if (ret != null) {
				this.writeJSON(resp, ret);
			}
		}
		else {
			handleEditAddReportal(req, resp, session);
		}
	}

	private void preparePage(Page page) {
		page.add("viewerName", viewerName);
	}

}
