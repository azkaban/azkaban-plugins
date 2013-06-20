package azkaban.triggertype.HdfsDataTrigger;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.session.Session;

public class HdfsDataTriggerServelet extends LoginAbstractAzkabanServlet{

	private static final long serialVersionUID = 1L;
	
	private HdfsDataTriggerManager hdfsDataTriggerManager;
	
	public HdfsDataTriggerServelet(Props props, AzkabanWebServer azkabanWebApp) {
		hdfsDataTriggerManager = new HdfsDataTriggerManager(props, azkabanWebApp.getTriggerManager(), azkabanWebApp.getExecutorManager(), azkabanWebApp.getProjectManager());
	}
	
	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
	}

}
