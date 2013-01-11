package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.webapp.servlet.AbstractViewerServlet;
import azkaban.webapp.servlet.Page;
import azkaban.webapp.session.Session;

public class HdfsBrowserServlet extends AbstractViewerServlet {
	private static final long serialVersionUID = 1L;
	private static final String PROXY_USER_SESSION_KEY = "hdfs.browser.proxy.user";
	private static Logger logger = Logger.getLogger(HdfsBrowserServlet.class);
	private static UserGroupInformation loginUser = null;

	private ArrayList<HdfsFileViewer> viewers = new ArrayList<HdfsFileViewer>();

	// Default viewer will be a text viewer
	private HdfsFileViewer defaultViewer;

	private Props props;
	private URI hdfsURI;
	private String proxyUser;
	private String keytabLocation;
	private boolean shouldProxy;
	private boolean allowGroupProxy;
	private Configuration conf;
	
	public HdfsBrowserServlet(String name, Props props) {
		this.props = props;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		try {
			hdfsURI = new URI(props.getString("hdfs.namenode.url"));
		} catch (URISyntaxException e) {
			logger.error(e);
		}

		conf = new Configuration();
		
		shouldProxy = props.getBoolean("azkaban.should.proxy", false);
		if (shouldProxy) {
			proxyUser = props.getString("proxy.user");
			keytabLocation = props.getString("proxy.keytab.location");
			allowGroupProxy = props.getBoolean("allow.group.proxy", false);

			logger.info("No login user. Creating login user");
			UserGroupInformation.setConfiguration(conf);
			try {
				UserGroupInformation.loginUserFromKeytab(proxyUser, keytabLocation);
				loginUser = UserGroupInformation.getLoginUser();
			} catch (IOException e) {
				logger.error("Error setting up hdfs browser security", e);
			}
		}

		defaultViewer = new TextFileViewer();
		viewers.add(new HdfsAvroFileViewer());
		viewers.add(new JsonSequenceFileViewer());
		viewers.add(defaultViewer);

		logger.info("HDFS Browser initiated");
	}

	private FileSystem getFileSystem(String username) throws IOException {
		UserGroupInformation ugi = getProxiedUser(username, new Configuration());
		FileSystem fs = ugi.doAs(new PrivilegedAction<FileSystem>(){
			@Override
			public FileSystem run() {
				try {
					return FileSystem.get(hdfsURI, conf);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		});
		
		return fs;
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		User user = session.getUser();
		String username = user.getUserId();
		
		if (allowGroupProxy) {
			String proxyName = (String)session.getSessionData(PROXY_USER_SESSION_KEY);
			if (proxyName != null) {
				username = proxyName;
			}
		}

		try {
			FileSystem fs = getFileSystem(username);
			try {
				handleFSDisplay(fs, username, req, resp, session);
			} catch (IOException e) {
				throw e;
			} finally {
				fs.close();
			}
		}
		catch (Exception e) {
			Page page = newPage(req, resp, session, "azkaban/hdfsviewer/hdfsbrowserpage.vm");
			page.add("error_message", "Error: " + e.getMessage());
			page.add("no_fs", "true");
			page.render();
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		// if (hasParam(req, "logout")) {
		// Page page = newPage(req, resp, session,
		// "azkaban/hdfsviewer/hdfsbrowserlogin.vm");
		// page.render();
		// } else if(hasParam(req, "login")) {
		// // Props prop = this.getApplication().getAzkabanProps();
		// // Properties property = prop.toProperties();
		//
		// String user = session.getUser().getUserId();
		//
		// String browseUser = getParam(req, "login");
		// logger.info("Browsing user set to " + browseUser + " by " + user);
		//
		//
		// try {
		// FileSystem fs;// = hadoopSecurityManager.getFSAsUser(user);
		//
		// try{
		// handleFSDisplay(fs, browseUser, req, resp, session);
		// } catch (IOException e) {
		// fs.close();
		// throw e;
		// } finally {
		// fs.close();
		// }
		//
		// } catch (Exception e) {
		// Page page = newPage(req, resp, session,
		// "azkaban/hdfsviewer/hdfsbrowserpage.vm");
		// page.add("error_message", "Error: " + e.getMessage());
		// page.add("no_fs", "true");
		// page.render();
		// }
		//
		// }
	}

	private void handleFSDisplay(FileSystem fs, String user, HttpServletRequest req, HttpServletResponse resp,
			Session session) throws IOException {
		String prefix = req.getContextPath() + req.getServletPath();
		String fsPath = req.getRequestURI().substring(prefix.length());
		if (fsPath.length() == 0)
			fsPath = "/";

		if (logger.isDebugEnabled())
			logger.debug("path=" + fsPath);

		Path path = new Path(fsPath);
		if (!fs.exists(path)) {
			throw new IllegalArgumentException(path.toUri().getPath() + " does not exist.");
		} else if (fs.isFile(path)) {
			displayFile(fs, req, resp, session, path);
		} else if (fs.getFileStatus(path).isDir()) {
			displayDir(fs, user, req, resp, session, path);
		} else {
			throw new IllegalStateException(
					"It exists, it is not a file, and it is not a directory, what is it precious?");
		}
	}

	private void displayDir(FileSystem fs, String user, HttpServletRequest req, HttpServletResponse resp,
			Session session, Path path) throws IOException {

		Page page = newPage(req, resp, session, "azkaban/viewer/hdfs/hdfsbrowserpage.vm");

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

		try {
			page.add("subdirs", fs.listStatus(path)); // ??? line
		} catch (AccessControlException e) {
			page.add("error_message", "Permission denied. User cannot read file or directory");
		} catch (IOException e) {
			page.add("error_message", "Error: " + e.getMessage());
		}
		page.render();

	}

	private void displayFile(FileSystem fs, HttpServletRequest req, HttpServletResponse resp, Session session, Path path)
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
			} else {
				output.write(("Sorry, no viewer available for this file. ").getBytes("UTF-8"));
			}
		}
	}

	/**
	 * Create a proxied user based on the explicit user name, taking other
	 * parameters necessary from properties file.
	 */
	public static synchronized UserGroupInformation getProxiedUser(String toProxy, Configuration conf) throws IOException {
		if (toProxy == null) {
			throw new IllegalArgumentException("toProxy can't be null");
		}
		if (conf == null) {
			throw new IllegalArgumentException("conf can't be null");
		}

		logger.info("loginUser (" + loginUser + ") already created, refreshing tgt.");
		loginUser.checkTGTAndReloginFromKeytab();

		return UserGroupInformation.createProxyUser(toProxy, loginUser);
	}

}