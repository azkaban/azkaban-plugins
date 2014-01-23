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

package azkaban.viewer.reportal;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.tools.generic.EscapeTool;
import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
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

public class ReportalServlet extends LoginAbstractAzkabanServlet {
	private static final String REPORTAL_VARIABLE_PREFIX = "reportal.variable.";
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(ReportalServlet.class);

	private CleanerThread cleanerThread;
	private File reportalMailDirectory;

	private AzkabanWebServer server;
	private Props props;
	private boolean shouldProxy;

	private String viewerName;
	private String reportalStorageUser;
	private File webResourcesFolder;
	private int itemsPerPage = 20;
	private boolean showNav;

	// private String viewerPath;
	private HadoopSecurityManager hadoopSecurityManager;

	public ReportalServlet(Props props) {
		this.props = props;

		viewerName = props.getString("viewer.name");
		reportalStorageUser = props.getString("reportal.storage.user", "reportal");
		itemsPerPage = props.getInt("reportal.items_per_page", 20);
		showNav = props.getBoolean("reportal.show.navigation", false);
		reportalMailDirectory = new File(props.getString("reportal.mail.temp.directory", "/tmp/reportal"));
		reportalMailDirectory.mkdirs();
		ReportalMailCreator.reportalMailDirectory = reportalMailDirectory;
		ReportalMailCreator.outputLocation = props.getString("reportal.output.location", "/tmp/reportal");
		ReportalMailCreator.outputFileSystem = props.getString("reportal.output.filesystem", "local");
		ReportalMailCreator.reportalStorageUser = reportalStorageUser;
		webResourcesFolder = new File(new File(props.getSource()).getParentFile().getParentFile(), "web");
		webResourcesFolder.mkdirs();
		setResourceDirectory(webResourcesFolder);
		System.out.println("Reportal web resources: " + webResourcesFolder.getAbsolutePath());
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		server = (AzkabanWebServer)getApplication();
		cleanerThread = new CleanerThread();
		cleanerThread.start();
		ReportalMailCreator.azkaban = server;

		shouldProxy = props.getBoolean("azkaban.should.proxy", false);
		logger.info("Hdfs browser should proxy: " + shouldProxy);
		try {
			hadoopSecurityManager = loadHadoopSecurityManager(props, logger);
			ReportalMailCreator.hadoopSecurityManager = hadoopSecurityManager;
		} catch (RuntimeException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
		}
	}

	private HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger logger) throws RuntimeException {

		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true, ReportalServlet.class.getClassLoader());
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
			if (!wasSubscribed && reportal.getAccessViewers().size() > 0 && !project.hasPermission(user, Type.READ)) {
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
			if (reportal.getAccessViewers().size() > 0 && !project.hasPermission(user, Type.READ)) {
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

	private void handleListReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportallistpage.vm");
		preparePage(page, session);

		List<Project> projects = ReportalHelper.getReportalProjects(server);
		page.add("ReportalHelper", ReportalHelper.class);
		page.add("esc", new EscapeTool());
		page.add("user", session.getUser());

		String startDate = DateTime.now().minusWeeks(1).toString("yyyy-MM-dd");
		String endDate = DateTime.now().toString("yyyy-MM-dd");
		page.add("startDate", startDate);
		page.add("endDate", endDate);

		if (!projects.isEmpty()) {
			page.add("projects", projects);
		}
		else {
			page.add("projects", false);
		}

		page.render();
	}

	private void handleViewReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, Exception {
		int id = getIntParam(req, "id");
		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportaldatapage.vm");
		preparePage(page, session);

		ProjectManager projectManager = server.getProjectManager();
		ExecutorManagerAdapter executorManager = server.getExecutorManager();

		Project project = projectManager.getProject(id);
		Reportal reportal = Reportal.loadFromProject(project);
		
		if (reportal == null) {
			page.add("errorMsg", "Report not found.");
			page.render();
			return;
		}

		if (reportal.getAccessViewers().size() > 0 && !project.hasPermission(session.getUser(), Type.READ)) {
			page.add("errorMsg", "You are not allowed to view this report.");
			page.render();
			return;
		}

		page.add("project", project);
		page.add("title", project.getMetadata().get("title"));

		if (hasParam(req, "execid")) {
			int execId = getIntParam(req, "execid");
			page.add("execid", execId);
			// Show logs
			if (hasParam(req, "logs")) {
				ExecutableFlow exec;
				try {
					exec = executorManager.getExecutableFlow(execId);
				} catch (ExecutorManagerException e) {
					e.printStackTrace();
					page.add("errorMsg", "ExecutableFlow not found. " + e.getMessage());
					page.render();
					return;
				}
				// View single log
				if (hasParam(req, "log")) {
					page.add("view-log", true);
					String jobId = getParam(req, "log");
					page.add("execid", execId);
					page.add("jobId", jobId);
				}
				// List files
				else {
					page.add("view-logs", true);
					List<ExecutableNode> jobs = exec.getExecutableNodes();
					
					// Add jobs in execution order
					List<ExecutableNode> logList = new ArrayList<ExecutableNode>();
					String prevNodeId = null;
					boolean showDataCollector = hasParam(req, "debug");
					int numJobs = jobs.size();
					for (int i = 0; i < numJobs; i++) {
						for (int j = 0; j < jobs.size(); j++) {
							ExecutableNode job = jobs.get(j);
							
							if (!showDataCollector && job.getId().equals("data-collector")) {
								jobs.remove(j);
								break;
							}
							
							Set<String> inNodes = job.getInNodes();
							String inNodeId = inNodes.size() == 0 ? null : inNodes.iterator().next();
							if ((inNodeId == prevNodeId) || (inNodeId != null && inNodeId.equals(prevNodeId))) {
								logList.add(job);
								jobs.remove(j);
								prevNodeId = job.getId();
								break;
							}
						}
					}
					
					if (logList.size() == 1) {
						resp.sendRedirect("/reportal?view&logs&id=" + project.getId() + "&execid=" + execId + "&log=" + logList.get(0).getId());
					}
					page.add("logs", logList);
				}
			}
			// Show data files
			else {
				String outputFileSystem = props.getString("reportal.output.filesystem", "local");
				String outputBase = props.getString("reportal.output.location", "/tmp/reportal");

				String locationFull = (outputBase + "/" + execId).replace("//", "/");

				IStreamProvider streamProvider = ReportalUtil.getStreamProvider(outputFileSystem);

				if (streamProvider instanceof StreamProviderHDFS) {
					StreamProviderHDFS hdfsStreamProvider = (StreamProviderHDFS)streamProvider;
					hdfsStreamProvider.setHadoopSecurityManager(hadoopSecurityManager);
					hdfsStreamProvider.setUser(reportalStorageUser);
				}

				try {
					// Preview file
					if (hasParam(req, "preview")) {
						page.add("view-preview", true);
						String fileName = getParam(req, "preview");
						String filePath = locationFull + "/" + fileName;
						InputStream csvInputStream = streamProvider.getFileInputStream(filePath);
						Scanner rowScanner = new Scanner(csvInputStream);
						ArrayList<Object> lines = new ArrayList<Object>();
						int lineNumber = 0;
						while (rowScanner.hasNextLine() && lineNumber < 100) {
							String csvLine = rowScanner.nextLine();
							String[] data = csvLine.split("\",\"");
							ArrayList<String> line = new ArrayList<String>();
							for (String item: data) {
								line.add(item.replace("\"", ""));
							}
							lines.add(line);
							lineNumber++;
						}
						rowScanner.close();
						page.add("preview", lines);
						Object graphHash = project.getMetadata().get("graphHash");
						if (graphHash != null) {
							page.add("graphHash", graphHash);
						}
					}
					else if (hasParam(req, "download")) {
						String fileName = getParam(req, "download");
						String filePath = locationFull + "/" + fileName;
						InputStream csvInputStream = null;
						OutputStream out = null;
						try {
							csvInputStream = streamProvider.getFileInputStream(filePath);
							resp.setContentType("application/octet-stream");

							out = resp.getOutputStream();
							IOUtils.copy(csvInputStream, out);
						} finally {
							IOUtils.closeQuietly(out);
							IOUtils.closeQuietly(csvInputStream);
						}
						return;
					}
					// List files
					else {
						page.add("view-files", true);
						try {
							String[] fileList = streamProvider.getFileList(locationFull);
							String[] dataList = ReportalHelper.filterCSVFile(fileList);
							Arrays.sort(dataList);
							if (dataList.length > 0) {
								page.add("files", dataList);
							}
						} catch (FileNotFoundException e) {

						}
					}
				} finally {
					streamProvider.cleanUp();
				}
			}
		}
		// List executions and their data
		else {
			page.add("view-executions", true);
			ArrayList<ExecutableFlow> exFlows = new ArrayList<ExecutableFlow>();

			int pageNumber = 0;
			boolean hasNextPage = false;
			if (hasParam(req, "page")) {
				pageNumber = getIntParam(req, "page") - 1;
			}
			if (pageNumber < 0) {
				pageNumber = 0;
			}
			try {
				Flow flow = project.getFlows().get(0);
				executorManager.getExecutableFlows(project.getId(), flow.getId(), pageNumber * itemsPerPage, itemsPerPage, exFlows);
				ArrayList<ExecutableFlow> tmp = new ArrayList<ExecutableFlow>();
				executorManager.getExecutableFlows(project.getId(), flow.getId(), (pageNumber + 1) * itemsPerPage, 1, tmp);
				if (!tmp.isEmpty()) {
					hasNextPage = true;
				}
			} catch (ExecutorManagerException e) {
				page.add("error", "Error retrieving executable flows");
			}

			if (!exFlows.isEmpty()) {
				ArrayList<Object> history = new ArrayList<Object>();
				for (ExecutableFlow exFlow: exFlows) {
					HashMap<String, Object> flowInfo = new HashMap<String, Object>();
					flowInfo.put("execId", exFlow.getExecutionId());
					flowInfo.put("status", exFlow.getStatus().toString());
					flowInfo.put("startTime", exFlow.getStartTime());

					history.add(flowInfo);
				}
				page.add("executions", history);
			}
			if (pageNumber > 0) {
				page.add("pagePrev", pageNumber);
			}
			page.add("page", pageNumber + 1);
			if (hasNextPage) {
				page.add("pageNext", pageNumber + 2);
			}
		}

		page.render();
	}

	private void handleRunReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		int id = getIntParam(req, "id");
		ProjectManager projectManager = server.getProjectManager();
		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportalrunpage.vm");
		preparePage(page, session);

		Project project = projectManager.getProject(id);
		Reportal reportal = Reportal.loadFromProject(project);

		if (reportal == null) {
			page.add("errorMsg", "Report not found");
			page.render();
			return;
		}

		if (reportal.getAccessExecutors().size() > 0 && !project.hasPermission(session.getUser(), Type.EXECUTE)) {
			page.add("errorMsg", "You are not allowed to run this report.");
			page.render();
			return;
		}

		page.add("projectId", id);
		page.add("title", reportal.title);
		page.add("description", reportal.description);

		if (reportal.variables.size() > 0) {
			page.add("variableNumber", reportal.variables.size());
			page.add("variables", reportal.variables);
		}

		page.render();
	}

	private void handleNewReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportaleditpage.vm");
		preparePage(page, session);

		page.add("title", "");
		page.add("description", "");

		page.add("queryNumber", 1);

		List<Map<String, Object>> queryList = new ArrayList<Map<String, Object>>();
		page.add("queries", queryList);

		Map<String, Object> query = new HashMap<String, Object>();
		queryList.add(query);
		query.put("title", "");
		query.put("type", "");
		query.put("script", "");

		page.add("accessViewer", "");
		page.add("accessExecutor", "");
		page.add("accessOwner", "");
		page.add("notifications", "");
		page.add("failureNotifications", "");

		page.render();
	}

	private void handleEditReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		int id = getIntParam(req, "id");
		ProjectManager projectManager = server.getProjectManager();

		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportaleditpage.vm");
		preparePage(page, session);
		page.add("ReportalHelper", ReportalHelper.class);
		page.add("esc", new EscapeTool());

		Project project = projectManager.getProject(id);
		Reportal reportal = Reportal.loadFromProject(project);

		if (reportal == null) {
			page.add("errorMsg", "Report not found");
			page.render();
			return;
		}

		if (!project.hasPermission(session.getUser(), Type.ADMIN)) {
			page.add("errorMsg", "You are not allowed to edit this report.");
			page.render();
			return;
		}

		page.add("projectId", id);
		page.add("title", reportal.title);
		page.add("description", reportal.description);
		page.add("queryNumber", reportal.queries.size());
		page.add("queries", reportal.queries);
		page.add("variableNumber", reportal.variables.size());
		page.add("variables", reportal.variables);
		page.add("schedule", reportal.schedule);
		page.add("scheduleHour", reportal.scheduleHour);
		page.add("scheduleMinute", reportal.scheduleMinute);
		page.add("scheduleAmPm", reportal.scheduleAmPm);
		page.add("scheduleTimeZone", reportal.scheduleTimeZone);
		page.add("scheduleDate", reportal.scheduleDate);
		page.add("scheduleRepeat", reportal.scheduleRepeat);
		page.add("scheduleIntervalQuantity", reportal.scheduleIntervalQuantity);
		page.add("scheduleInterval", reportal.scheduleInterval);
		page.add("notifications", reportal.notifications);
		page.add("failureNotifications", reportal.failureNotifications);
		page.add("accessViewer", reportal.accessViewer);
		page.add("accessExecutor", reportal.accessExecutor);
		page.add("accessOwner", reportal.accessOwner);

		page.render();
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
		  handleSaveReportal(req, resp, session);		 
		}
	}
	
	private void handleSaveReportal(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
	  String projectId = validateAndSaveReport(req, resp, session); 
	  
    if (projectId != null) {
      this.setSuccessMessageInCookie(resp, "Report Saved.");
      
      String submitType = getParam(req, "submit");
      if (submitType.equals("Save")) {
        resp.sendRedirect(req.getRequestURI() + "?edit&id=" + projectId);
      } else {
        resp.sendRedirect(req.getRequestURI() + "?run&id=" + projectId);
      }
    }
	}

  /**
   * Validates and saves a report, returning the project id of the saved report if successful, and null otherwise.
   * @param req
   * @param resp
   * @param session
   * @return The project id of the saved report if successful, and null otherwise
   * @throws ServletException
   * @throws IOException
   */
	private String validateAndSaveReport(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

		ProjectManager projectManager = server.getProjectManager();
		Page page = newPage(req, resp, session, "azkaban/viewer/reportal/reportaleditpage.vm");
		preparePage(page, session);
		page.add("ReportalHelper", ReportalHelper.class);
		page.add("esc", new EscapeTool());

		boolean isEdit = hasParam(req, "id");
		if (isEdit) {
		  page.add("projectId", getIntParam(req, "id"));
		}
		
		Project project = null;
		Reportal report = new Reportal();

		report.title = getParam(req, "title");
		report.description = getParam(req, "description");
		page.add("title", report.title);
		page.add("description", report.description);

		report.schedule = hasParam(req, "schedule");
		report.scheduleHour = getParam(req, "schedule-hour");
		report.scheduleMinute = getParam(req, "schedule-minute");
		report.scheduleAmPm = getParam(req, "schedule-am_pm");
		report.scheduleTimeZone = getParam(req, "schedule-timezone");
		report.scheduleDate = getParam(req, "schedule-date");
		report.scheduleRepeat = hasParam(req, "schedule-repeat");
		report.scheduleIntervalQuantity = getParam(req, "schedule-interval-quantity");
		report.scheduleInterval = getParam(req, "schedule-interval");
		page.add("schedule", report.schedule);
		page.add("scheduleHour", report.scheduleHour);
		page.add("scheduleMinute", report.scheduleMinute);
		page.add("scheduleAmPm", report.scheduleAmPm);
		page.add("scheduleTimeZone", report.scheduleTimeZone);
		page.add("scheduleDate", report.scheduleDate);
		page.add("scheduleRepeat", report.scheduleRepeat);
		page.add("scheduleIntervalQuantity", report.scheduleIntervalQuantity);
		page.add("scheduleInterval", report.scheduleInterval);

		report.accessViewer = getParam(req, "access-viewer");
		report.accessExecutor = getParam(req, "access-executor");
		report.accessOwner = getParam(req, "access-owner");
		page.add("accessViewer", report.accessViewer);
		page.add("accessExecutor", report.accessExecutor);
		page.add("accessOwner", report.accessOwner);

		report.notifications = getParam(req, "notifications");
		report.failureNotifications = getParam(req, "failure-notifications");
		page.add("notifications", report.notifications);
		page.add("failureNotifications", report.failureNotifications);

		int numQueries = getIntParam(req, "queryNumber");
		page.add("queryNumber", numQueries);
		List<Query> queryList = new ArrayList<Query>(numQueries);
		page.add("queries", queryList);
		report.queries = queryList;

		String typeError = null;
		String typePermissionError = null;
		for (int i = 0; i < numQueries; i++) {
			Query query = new Query();

			query.title = getParam(req, "query" + i + "title");
			query.type = getParam(req, "query" + i + "type");
			query.script = getParam(req, "query" + i + "script");

			// Type check
			ReportalType type = ReportalType.getTypeByName(query.type);
			if (type == null && typeError == null) {
				typeError = query.type;
			}
			if (!type.checkPermission(session.getUser())) {
				typePermissionError = query.type;
			}

			queryList.add(query);
		}

		int variables = getIntParam(req, "variableNumber");
		page.add("variableNumber", variables);
		List<Variable> variableList = new ArrayList<Variable>(variables);
		page.add("variables", variableList);
		report.variables = variableList;

		boolean variableErrorOccurred = false;
		for (int i = 0; i < variables; i++) {
			Variable variable = new Variable();

			variable.title = getParam(req, "variable" + i + "title");
			variable.name = getParam(req, "variable" + i + "name");

			if (variable.title.isEmpty() || variable.name.isEmpty()) {
				variableErrorOccurred = true;
			}

			variableList.add(variable);
		}

		// Make sure title isn't empty
		if (report.title.isEmpty()) {
			page.add("errorMsg", "Title must not be empty.");
			page.render();
			return null;
		}

		// Make sure description isn't empty
		if (report.description.isEmpty()) {
			page.add("errorMsg", "Description must not be empty.");
			page.render();
			return null;
		}
		
		// Verify schedule and repeat
		if (report.schedule) {
			// Verify schedule time
			if (!NumberUtils.isDigits(report.scheduleHour) ||
					!NumberUtils.isDigits(report.scheduleMinute)) {
				page.add("errorMsg", "Schedule time is invalid.");
				page.render();
				return null;
			}
			
			// Verify schedule date is not empty
			if (report.scheduleDate.isEmpty()) {
				page.add("errorMsg", "Schedule date must not be empty.");
				page.render();
				return null;
			}
			
			if (report.scheduleRepeat) {
				// Verify repeat interval
				if (!NumberUtils.isDigits(report.scheduleIntervalQuantity)) {
					page.add("errorMsg", "Repeat interval quantity is invalid.");
					page.render();
					return null;
				}
			}
		}

		// Empty query check
		if (numQueries <= 0) {
			page.add("errorMsg", "There needs to have at least one query.");
			page.render();
			return null;
		}
		// Type error check
		if (typeError != null) {
			page.add("errorMsg", "Type " + typeError + " is invalid.");
			page.render();
			return null;
		}
		// Type permission check
		if (typePermissionError != null && report.schedule) {
			page.add("errorMsg", "You do not have permission to schedule Type " + typePermissionError + ".");
			page.render();
			return null;
		}
		// Variable error check
		if (variableErrorOccurred) {
			page.add("errorMsg", "Variable title and name cannot be empty.");
			page.render();
			return null;
		}

		// Attempt to get a project object
		if (isEdit) {
			// Editing mode, load project
			int projectId = getIntParam(req, "id");
			project = projectManager.getProject(projectId);
			report.loadImmutableFromProject(project);
		}
		else {
			// Creation mode, create project
			try {
				User user = session.getUser();
				project = ReportalHelper.createReportalProject(server, report.title, report.description, user);
				report.reportalUser = user.getUserId();
				report.ownerEmail = user.getEmail();
			} catch (Exception e) {
				e.printStackTrace();
				page.add("errorMsg", "Error while creating report. " + e.getMessage());
				page.render();
				return null;
			}

			// Project already exists
			if (project == null) {
				page.add("errorMsg", "A Report with the same name already exists.");
				page.render();
				return null;
			}
		}

		if (project == null) {
			page.add("errorMsg", "Internal Error: Report not found");
			page.render();
			return null;
		}

		report.project = project;
		page.add("projectId", project.getId());

		report.updatePermissions();

		try {
			report.createZipAndUpload(projectManager, session.getUser(), reportalStorageUser);
		} catch (Exception e) {
			e.printStackTrace();
			page.add("errorMsg", "Error while creating Azkaban jobs. " + e.getMessage());
			page.render();
			if (!isEdit) {
				try {
					projectManager.removeProject(project, session.getUser());
				} catch (ProjectManagerException e1) {
					e1.printStackTrace();
				}
			}
			return null;
		}

		// Prepare flow
		Flow flow = project.getFlows().get(0);
		project.getMetadata().put("flowName", flow.getId());

		// Set reportal mailer
		flow.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

		// Create/Save schedule
		ScheduleManager scheduleManager = server.getScheduleManager();
		try {
			report.updateSchedules(report, scheduleManager, session.getUser(), flow);
		} catch (ScheduleManagerException e2) {
			e2.printStackTrace();
			page.add("errorMsg", e2.getMessage());
			page.render();
			return null;
		}

		report.saveToProject(project);

		try {
			ReportalHelper.updateProjectNotifications(project, projectManager);
			projectManager.updateProjectSetting(project);
			projectManager.updateFlow(project, flow);
		} catch (ProjectManagerException e) {
			e.printStackTrace();
			page.add("errorMsg", "Error while updating report. " + e.getMessage());
			page.render();
			if (!isEdit) {
				try {
					projectManager.removeProject(project, session.getUser());
				} catch (ProjectManagerException e1) {
					e1.printStackTrace();
				}
			}
			return null;
		}

		return Integer.toString(project.getId());
	}

	private void handleRunReportalWithVariables(HttpServletRequest req, HashMap<String, Object> ret, Session session) throws ServletException, IOException {
		int id = getIntParam(req, "id");
		ProjectManager projectManager = server.getProjectManager();
		Project project = projectManager.getProject(id);
		Reportal report = Reportal.loadFromProject(project);
		User user = session.getUser();

		if (report.getAccessExecutors().size() > 0 && !project.hasPermission(user, Type.EXECUTE)) {
			ret.put("error", "You are not allowed to run this report.");
			return;
		}

		for (Query query: report.queries) {
			String jobType = query.type;
			ReportalType type = ReportalType.getTypeByName(jobType);
			if (!type.checkPermission(user)) {
				ret.put("error", "You are not allowed to run this report as you don't have permission to run job type " + type.toString() + ".");
				return;
			}
		}

		Flow flow = project.getFlows().get(0);

		ExecutableFlow exflow = new ExecutableFlow(project, flow);
		exflow.setSubmitUser(user.getUserId());
		exflow.addAllProxyUsers(project.getProxyUsers());

		ExecutionOptions options = exflow.getExecutionOptions();

		int i = 0;
		for (Variable variable: report.variables) {
			options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".from", variable.name);
			options.getFlowParameters().put(REPORTAL_VARIABLE_PREFIX + i + ".to", getParam(req, "variable" + i));
			i++;
		}
		
		options.getFlowParameters().put("reportal.execution.user", user.getUserId());
		
		// Add the execution user's email to the list of success and failure emails.
		String email = user.getEmail();
		if (email != null && !email.isEmpty()) {
			options.getSuccessEmails().add(email);
			options.getFailureEmails().add(email);
		}
		
		options.getFlowParameters().put("reportal.title", report.title);
		
		options.getFlowParameters().put("reportal.unscheduled.run", "true");

		try {
			String message = server.getExecutorManager().submitExecutableFlow(exflow, session.getUser().getUserId()) + ".";
			ret.put("message", message);
			ret.put("result", "success");
			ret.put("redirect", "/reportal?view&logs&id=" + project.getId() + "&execid=" + exflow.getExecutionId());
		} catch (ExecutorManagerException e) {
			e.printStackTrace();
			ret.put("error", "Error running report " + report.title + ". " + e.getMessage());
		}
	}

	private void preparePage(Page page, Session session) {
		page.add("viewerName", viewerName);
		page.add("hideNavigation", !showNav);
		page.add("userid", session.getUser().getUserId());
	}

	private class CleanerThread extends Thread {
		// Every hour, clean execution dir.
		private static final long EXECUTION_DIR_CLEAN_INTERVAL_MS = 60 * 60 * 1000;

		private boolean shutdown = false;
		private long lastExecutionDirCleanTime = -1;
		private long executionDirRetention = 1 * 24 * 60 * 60 * 1000;

		public CleanerThread() {
			this.setName("Reportal-Cleaner-Thread");
		}

		@SuppressWarnings("unused")
		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}

		public void run() {
			while (!shutdown) {
				synchronized (this) {
					long currentTime = System.currentTimeMillis();
					if (currentTime - EXECUTION_DIR_CLEAN_INTERVAL_MS > lastExecutionDirCleanTime) {
						logger.info("Cleaning old execution dirs");
						cleanOldReportalDirs();
						lastExecutionDirCleanTime = currentTime;
					}
				}
			}
		}

		private void cleanOldReportalDirs() {
			File dir = reportalMailDirectory;

			final long pastTimeThreshold = System.currentTimeMillis() - executionDirRetention;
			File[] executionDirs = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File path) {
					if (path.isDirectory() && path.lastModified() < pastTimeThreshold) {
						return true;
					}
					return false;
				}
			});

			for (File exDir: executionDirs) {
				try {
					FileUtils.deleteDirectory(exDir);
				} catch (IOException e) {
					logger.error("Error cleaning execution dir " + exDir.getPath(), e);
				}
			}
		}
	}
}
