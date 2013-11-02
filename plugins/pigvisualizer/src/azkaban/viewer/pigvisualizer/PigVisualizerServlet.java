package azkaban.viewer.pigvisualizer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.user.User;
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


	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			handleAJAXAction(req, resp, session);
		}
	}

	private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String ajaxName = getParam(req, "ajax");
		User user = session.getUser();
		int id = getIntParam(req, "id");
		ProjectManager projectManager = server.getProjectManager();
		Project project = projectManager.getProject(id);

		if (ret != null) {
			this.writeJSON(resp, ret);
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
		if (hasParam(req, "ajax")) {
			HashMap<String, Object> ret = new HashMap<String, Object>();

			if (ret != null) {
				this.writeJSON(resp, ret);
			}
		}
		else {
			
		}
	}

	private void preparePage(Page page) {
		page.add("viewerName", viewerName);
	}

}
