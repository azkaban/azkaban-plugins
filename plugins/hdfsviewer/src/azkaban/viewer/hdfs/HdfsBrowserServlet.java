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

package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.session.Session;

public class HdfsBrowserServlet extends LoginAbstractAzkabanServlet {
	private static final long serialVersionUID = 1L;
	private static final String PROXY_USER_SESSION_KEY = "hdfs.browser.proxy.user";
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	private static Logger logger = Logger.getLogger(HdfsBrowserServlet.class);

	private ArrayList<HdfsFileViewer> viewers = new ArrayList<HdfsFileViewer>();

	private HdfsFileViewer defaultViewer;

	private Props props;
	private boolean shouldProxy;
	private boolean allowGroupProxy;
	
	private String viewerName;
	private String viewerPath;
	
	private HadoopSecurityManager hadoopSecurityManager;
	
	public HdfsBrowserServlet(Props props) {
		this.props = props;
		
		viewerName = props.getString("viewer.name");
		viewerPath = props.getString("viewer.path");
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		shouldProxy = props.getBoolean("azkaban.should.proxy", false);
		allowGroupProxy = props.getBoolean("allow.group.proxy", false);
		logger.info("Hdfs browser should proxy: " + shouldProxy);
		
		props.put("fs.hdfs.impl.disable.cache", "true");
		
		try {
			hadoopSecurityManager = loadHadoopSecurityManager(props, logger);
		}
		catch(RuntimeException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
		}
		
		defaultViewer = new TextFileViewer();
		viewers.add(new AvroFileViewer());
		viewers.add(new ParquetFileViewer());
		viewers.add(new JsonSequenceFileViewer());
		viewers.add(new ImageFileViewer());
		viewers.add(new BsonFileViewer());
		viewers.add(defaultViewer);

		logger.info("HDFS Browser initiated");
	}
	
	private HadoopSecurityManager loadHadoopSecurityManager(Props props, Logger logger) throws RuntimeException {

		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true, HdfsBrowserServlet.class.getClassLoader());
		logger.info("Initializing hadoop security manager " + hadoopSecurityManagerClass.getName());
		HadoopSecurityManager hadoopSecurityManager = null;

		try {
			Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
			hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(hadoopSecurityManagerClass, props);
		} 
		catch (InvocationTargetException e) {
			logger.error("Could not instantiate Hadoop Security Manager "+ hadoopSecurityManagerClass.getName() + e.getCause());
			throw new RuntimeException(e.getCause());
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getCause());
		}

		return hadoopSecurityManager;
	}

	private FileSystem getFileSystem(String username)
			throws HadoopSecurityManagerException {
		return hadoopSecurityManager.getFSAsUser(username);
	}
	
	@Override
	protected void handleGet(
			HttpServletRequest req, 
			HttpServletResponse resp, 
			Session session) throws ServletException, IOException {
		User user = session.getUser();
		String username = user.getUserId();
	
		if (hasParam(req, "action") && getParam(req, "action").equals("goHomeDir")) {
			username = getParam(req, "proxyname");
		}
		else if (allowGroupProxy) {
			String proxyName = (String)session.getSessionData(PROXY_USER_SESSION_KEY);
			if (proxyName != null) {
				username = proxyName;
			}
		}

		FileSystem fs = null;
		try {
			fs = getFileSystem(username);
			try {
				if (hasParam(req, "ajax")) {
					handleAjaxAction(fs, username, req, resp, session);
				}
				else {
					handleFsDisplay(fs, username, req, resp, session);
				}
			}
			catch (IOException e) {
				throw e;
			}
			catch (ServletException se) {
				throw se;
			}
			catch (Exception ge) {
				throw ge;
			}
			finally {
				if(fs != null) {
					fs.close();
				}
			}
		}
		catch (Exception e) {
			//e.printStackTrace();
			Page page = newPage(req, resp, session, "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
			page.add("error_message", "Error: " + e.getMessage());
			page.add("user", username);
			page.add("allowproxy", allowGroupProxy);
			page.add("no_fs", "true");
			page.add("viewerName", viewerName);
			page.render();
		}
		finally {
			if (fs != null) {
				fs.close();
			}
		}
	}

	@Override
	protected void handlePost(
			HttpServletRequest req, 
			HttpServletResponse resp, 
			Session session) throws ServletException, IOException {
		User user = session.getUser();
		if (!hasParam(req, "action")) {
			return;
		}

		HashMap<String,String> results = new HashMap<String,String>();			
		String action = getParam(req, "action");
		
		if (action.equals("changeProxyUser")) {
			if (hasParam(req, "proxyname")) {
				String newProxyname = getParam(req, "proxyname");
							
				if (user.getUserId().equals(newProxyname) || 
						user.isInGroup(newProxyname) || 
						user.getRoles().contains("admin")) {
					session.setSessionData(PROXY_USER_SESSION_KEY, newProxyname);
				}
				else {
					results.put("error", "User '" + user.getUserId() + 
							"' cannot proxy as '" + newProxyname + "'");
				}
			}
		}
		else {
			results.put("error", "action param is not set");
		}
		
		this.writeJSON(resp, results);
	}
		
	private void handleFsDisplay(
			FileSystem fs, 
			String user, 
			HttpServletRequest req, 
			HttpServletResponse resp,
			Session session) 
			throws IOException, 
					ServletException, 
					IllegalArgumentException, 
					IllegalStateException {

		String prefix = req.getContextPath() + req.getServletPath();
		String fsPath = req.getRequestURI().substring(prefix.length());
		
		Path path;
		if (fsPath.length() == 0) {
			fsPath = "/";
		}
		path = new Path(fsPath);

		if (logger.isDebugEnabled())
			logger.debug("path=" + path.toString());

		if (!fs.exists(path)) {
			throw new IllegalArgumentException(path.toUri().getPath() + " does not exist.");
		}
		else if (fs.isFile(path)) {
			displayFilePage(fs, user, req, resp, session, path);
		}
		else if (fs.getFileStatus(path).isDir()) {
			displayDirPage(fs, user, req, resp, session, path);
		}
		else {
			throw new IllegalStateException(
					"It exists, it is not a file, and it is not a directory, what is it precious?");
		}
	}

	private void displayDirPage(
			FileSystem fs, 
			String user, 
			HttpServletRequest req, 
			HttpServletResponse resp,
			Session session, 
			Path path) throws IOException {

		Page page = newPage(req, resp, session, "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
		page.add("allowproxy", allowGroupProxy);
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);
		
		List<Path> paths = new ArrayList<Path>();
		List<String> segments = new ArrayList<String>();
		Path curr = path;
		while (curr.getParent() != null) {
			paths.add(curr);
			segments.add(curr.getName());
			curr = curr.getParent();
		}

		Collections.reverse(paths);
		Collections.reverse(segments);

		page.add("paths", paths);
		page.add("segments", segments);
		page.add("user", user);
		
		String homeDirString = fs.getHomeDirectory().toString();
		if (homeDirString.startsWith("file:")) {
			page.add("homedir", homeDirString.substring("file:".length()));
		}
		else {
			page.add("homedir", homeDirString.substring(fs.getUri().toString().length()));
		}

		try {
			FileStatus[] subdirs = fs.listStatus(path);
			page.add("subdirs", subdirs);
			long size = 0;
			for (int i = 0; i < subdirs.length; ++i) {
				if (subdirs[i].isDir()) {
					continue;
				}
				size += subdirs[i].getLen();
			}
			page.add("dirsize", size);
		}
		catch (AccessControlException e) {
			page.add("error_message", "Permission denied. User cannot read file or directory");
		}
		catch (IOException e) {
			page.add("error_message", "Error: " + e.getMessage());
		}
		page.render();
	}

	private void displayFilePage(
			FileSystem fs, 
			String user,
			HttpServletRequest req, 
			HttpServletResponse resp, 
			Session session, 
			Path path)
			throws IOException {
		Page page = newPage(req, resp, session, "azkaban/viewer/hdfs/velocity/hdfs-file.vm");
		page.add("allowproxy", allowGroupProxy);
		page.add("viewerPath", viewerPath);
		page.add("viewerName", viewerName);
		
		List<Path> paths = new ArrayList<Path>();
		List<String> segments = new ArrayList<String>();
		Path curr = path;
		while (curr.getParent() != null) {
			paths.add(curr);
			segments.add(curr.getName());
			curr = curr.getParent();
		}

		Collections.reverse(paths);
		Collections.reverse(segments);

		page.add("paths", paths);
		page.add("segments", segments);
		page.add("user", user);
		
		String homeDirString = fs.getHomeDirectory().toString();
		if (homeDirString.startsWith("file:")) {
			page.add("homedir", homeDirString.substring("file:".length()));
		}
		else {
			page.add("homedir", homeDirString.substring(fs.getUri().toString().length()));
		}

		try {
			FileStatus status = fs.getFileStatus(path);
			page.add("status", status);
		}
		catch (AccessControlException e) {
			page.add("error_message", "Permission denied. User annot read this file.");
		}
		catch (IOException e) {
			page.add("error_message", "Error: " + e.getMessage());
		}
		page.render();
	}
	
	private void handleAjaxAction(
			FileSystem fs,
			String username,
			HttpServletRequest request,
			HttpServletResponse response,
			Session session) 
			throws ServletException, IOException {

		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(request, "ajax");
		Path path = null;
		if (!hasParam(request, "path")) {
			ret.put("error", "Missing parameter 'path'.");
			this.writeJSON(response, ret);
			return;
		}

		path = new Path(getParam(request, "path"));
		if (!fs.exists(path)) {
			ret.put("error", path.toUri().getPath() + " does not exist.");
			this.writeJSON(response, ret);
			return;
		}

		if (ajaxName.equals("fetchschema")) {
			handleAjaxFetchSchema(fs, request, response, session, path);
		}
		else if (ajaxName.equals("fetchfile")) {
			handleAjaxFetchFile(fs, request, response, session, path);
		}
		else {
			ret.put("error", "Unknown AJAX action " + ajaxName);
		}

		if (ret != null) {
			this.writeJSON(response, ret);
		}
	}

	private void handleAjaxFetchSchema(
			FileSystem fs, 
			HttpServletRequest req, 
			HttpServletResponse resp, 
			Session session, 
			Path path)
			throws IOException {

		// use registered viewers to show the file content
		boolean outputed = false;
		String schema = null;
		
		for (HdfsFileViewer viewer : viewers) {
			if (viewer.canReadSchema(fs, path)) {
				schema = viewer.getSchema(fs, path);
				outputed = true;
				break; // don't need to try other viewers
			}
		}
		
	}

	private void handleAjaxFetchFile(
			FileSystem fs, 
			HttpServletRequest req, 
			HttpServletResponse resp, 
			Session session, 
			Path path)
			throws IOException {
		int startLine = getIntParam(req, "start_line", 1);
		int endLine = getIntParam(req, "end_line", 1000);

		// use registered viewers to show the file content
		boolean outputed = false;
		OutputStream output = resp.getOutputStream();
		
		for (HdfsFileViewer viewer : viewers) {
			if (viewer.canReadFile(fs, path)) {
				viewer.displayFile(fs, path, output, startLine, endLine);
				outputed = true;
				break; // don't need to try other viewers
			}
		}
		
		// use default text viewer
		if (!outputed) {
			if (defaultViewer.canReadFile(fs, path)) {
				defaultViewer.displayFile(fs, path, output, startLine, endLine);
			}
			else {
				output.write(("Sorry, no viewer available for this file. ").getBytes("UTF-8"));
			}
		}
	}

//	/**
//	 * Create a proxied user based on the explicit user name, taking other
//	 * parameters necessary from properties file.
//	 */
//	public static synchronized UserGroupInformation getProxiedUser(String toProxy, Configuration conf, boolean shouldProxy) throws IOException {
//		if (toProxy == null) {
//			throw new IllegalArgumentException("toProxy can't be null");
//		}
//		if (conf == null) {
//			throw new IllegalArgumentException("conf can't be null");
//		}
//
//		if (shouldProxy) {
//			logger.info("loginUser (" + loginUser + ") already created, refreshing tgt.");
//			loginUser.checkTGTAndReloginFromKeytab();
//			return UserGroupInformation.createProxyUser(toProxy, loginUser);
//		}
//		
//		return UserGroupInformation.createRemoteUser(toProxy);
//	}

}

/* vim: set ai ts=2 sts=2 sw=2 noet: */
