package azkaban.triggertype.HdfsDataTrigger;

import org.mortbay.jetty.servlet.ServletHolder;

import org.mortbay.jetty.servlet.Context;
import azkaban.trigger.TriggerServicer;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.AbstractAzkabanServlet;
import azkaban.webapp.servlet.TriggerPlugin;

public class HdfsDataTriggerPlugin implements TriggerPlugin {

	private final HdfsDataTriggerManager hdfsDataTriggerManager;
	private final HdfsDataTriggerServelet hdfsDataTriggerServlet;
	
	public HdfsDataTriggerPlugin(String name, Props props, Context root, AzkabanWebServer azkabanWebApp) {
		
		HdfsDataTriggerManager hdfsDataTriggerServicer = new HdfsDataTriggerManager(props, azkabanWebApp.getTriggerManager(), azkabanWebApp.getExecutorManager(), azkabanWebApp.getProjectManager());
		HdfsDataTriggerServelet hdfsDataTriggerServlet = new HdfsDataTriggerServelet(props, azkabanWebApp);
		this.hdfsDataTriggerManager = hdfsDataTriggerServicer;
		this.hdfsDataTriggerServlet = hdfsDataTriggerServlet;
		
//		String pluginName = props.getString("viewer.name");
		String pluginWebPath = props.getString("trigger.path");
//		int pluginOrder = props.getInt("viewer.order", 0);
//		boolean pluginHidden = props.getBoolean("viewer.hidden", false);
		root.addServlet(new ServletHolder(hdfsDataTriggerServlet), "/" + pluginWebPath + "/*");
	}

	@Override
	public AbstractAzkabanServlet getServlet() {
		return hdfsDataTriggerServlet;
	}

	@Override
	public TriggerServicer getServicer() {
		return hdfsDataTriggerManager;
	}
	
	@Override
	public void load() {
		hdfsDataTriggerManager.load();
	}
	
	
}
