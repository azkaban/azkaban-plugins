package azkaban.viewer.reportal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.project.Project;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.ReportalHelper;
import azkaban.reportal.util.ReportalUtil;
import azkaban.reportal.util.StreamProviderHDFS;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.EmailMessage;
import azkaban.webapp.AzkabanWebServer;

public class ReportalMailCreator implements MailCreator {
	public static AzkabanWebServer azkaban = null;
	public static HadoopSecurityManager hadoopSecurityManager = null;
	public static String outputLocation = "";
	public static String outputFileSystem = "";
	public static String reportalStorageUser = "";
	public static File reportalMailDirectory;
	public static final String REPORTAL_MAIL_CREATOR = "ReportalMailCreator";
	static {
		DefaultMailCreator.registerCreator(REPORTAL_MAIL_CREATOR, new ReportalMailCreator());
	}

	@Override
	public boolean createFirstErrorMessage(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		List<String> emailList = option.getDisabledJobs();

		if (emailList != null && !emailList.isEmpty()) {
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Report '" + flow.getExecutionOptions().getFlowParameters().get("reportal.title") + "' has failed on " + azkabanName);
			String urlPrefix = "https://" + clientHostname + ":" + clientPortNumber + "/reportal";
			try {
				createMessage(flow, message, urlPrefix, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}

		return false;
	}

	@Override
	public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		List<String> emailList = option.getFailureEmails();

		if (emailList != null && !emailList.isEmpty()) {
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Report '" + flow.getExecutionOptions().getFlowParameters().get("reportal.title") + "' has failed on " + azkabanName);
			String urlPrefix = "https://" + clientHostname + ":" + clientPortNumber + "/reportal";
			try {
				createMessage(flow, message, urlPrefix, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	@Override
	public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		List<String> emailList = option.getSuccessEmails();

		if (emailList != null && !emailList.isEmpty()) {
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Report '" + flow.getExecutionOptions().getFlowParameters().get("reportal.title") + "' has succeeded on " + azkabanName);
			String urlPrefix = "https://" + clientHostname + ":" + clientPortNumber + "/reportal";
			try {
				createMessage(flow, message, urlPrefix, true);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	private void createMessage(ExecutableFlow flow, EmailMessage message, String urlPrefix, boolean printData) throws Exception {
		Project project = azkaban.getProjectManager().getProject(flow.getProjectId());
		message.println("<html>");
		message.println("<head></head>");
		message.println("<body style='font-family: verdana; color: #000000; background-color: #cccccc; padding: 20px;'>");
		message.println("<div style='background-color: #ffffff; border: 1px solid #aaaaaa; padding: 20px;-webkit-border-radius: 15px; -moz-border-radius: 15px; border-radius: 15px;'>");
		// Title
		message.println("<b>" + project.getMetadata().get("title") + "</b>");
		message.println("<div style='font-size: .8em; margin-top: .5em; margin-bottom: .5em;'>");
		// Status
		message.println(flow.getStatus().name());
		// Link to logs
		message.println("(<a href='" + urlPrefix + "?view&logs&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Logs</a>)");
		// Link to Data
		message.println("(<a href='" + urlPrefix + "?view&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Result data</a>)");
		// Link to Edit
		message.println("(<a href='" + urlPrefix + "?edit&id=" + flow.getProjectId() + "'>Edit</a>)");
		message.println("</div>");
		message.println("<div style='margin-top: .5em; margin-bottom: .5em;'>");
		// Description
		message.println(project.getDescription());
		message.println("</div>");

		// message.println("{% if queue.vars|length > 0 %}");
		// message.println("<div style='margin-top: 10px; margin-bottom: 10px; border-bottom: 1px solid #ccc; padding-bottom: 5px; font-weight: bold;'>");
		// message.println("Variables");
		// message.println("</div>");
		// message.println("");
		// message.println("<div>");
		// message.println("{% for qv in queue.vars %}");
		// message.println("{{ qv.var.title }}: {{ qv.value}}<br/>");
		// message.println("{% endfor %}");
		// message.println("</div>");
		// message.println("{% endif %}");

		if (printData) {
			String locationFull = (outputLocation + "/" + flow.getExecutionId()).replace("//", "/");

			IStreamProvider streamProvider = ReportalUtil.getStreamProvider(outputFileSystem);

			if (streamProvider instanceof StreamProviderHDFS) {
				StreamProviderHDFS hdfsStreamProvider = (StreamProviderHDFS) streamProvider;
				hdfsStreamProvider.setHadoopSecurityManager(hadoopSecurityManager);
				hdfsStreamProvider.setUser(reportalStorageUser);
			}

			// Get file list
			String[] fileList = ReportalHelper.filterCSVFile(streamProvider.getFileList(locationFull));

			File tempFolder = new File(reportalMailDirectory + "/" + flow.getExecutionId());
			tempFolder.mkdirs();

			for (String file : fileList) {
				String filePath = locationFull + "/" + file;
				InputStream csvInputStream = null;
				OutputStream tempOutputStream = null;
				File tempOutputFile = new File(tempFolder, file);
				tempOutputFile.createNewFile();
				try {
					csvInputStream = streamProvider.getFileInputStream(filePath);
					tempOutputStream = new BufferedOutputStream(new FileOutputStream(tempOutputFile));

					IOUtils.copy(csvInputStream, tempOutputStream);
				} finally {
					IOUtils.closeQuietly(tempOutputStream);
					IOUtils.closeQuietly(csvInputStream);
				}
			}

			try {
				streamProvider.cleanUp();
			} catch (IOException e) {
				e.printStackTrace();
			}

			for (String file : fileList) {
				message.println("<div style='margin-top: 10px; margin-bottom: 10px; border-bottom: 1px solid #ccc; padding-bottom: 5px; font-weight: bold;'>");
				message.println(file);
				message.println("</div>");
				message.println("<div>");
				message.println("<table border='1' cellspacing='0' cellpadding='2' style='font-size: 14px;'>");
				File tempOutputFile = new File(tempFolder, file);
				InputStream csvInputStream = null;
				try {
					csvInputStream = new BufferedInputStream(new FileInputStream(tempOutputFile));
					Scanner rowScanner = new Scanner(csvInputStream);
					int lineNumber = 0;
					while (rowScanner.hasNextLine() && lineNumber <= 20) {
						String csvLine = rowScanner.nextLine();
						String[] data = csvLine.split(",");
						message.println("<tr>");
						for (String item : data) {
							message.println("<td>" + item.replace("\"", "") + "</td>");
						}
						message.println("</tr>");
						if (lineNumber == 20 && rowScanner.hasNextLine()) {
							message.println("<tr>");
							message.println("<td colspan=\"" + data.length + "\">...</td>");
							message.println("</tr>");
						}
						lineNumber++;
					}
					rowScanner.close();
					message.println("</table>");
					message.println("</div>");
				} finally {
					IOUtils.closeQuietly(csvInputStream);
				}
				message.addAttachment(file, tempOutputFile);
			}
		}

		message.println("</div>").println("</body>").println("</html>");

		// message.println("<h2> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has succeeded on " + azkabanName + "</h2>");
		// message.println("<table>");
		// message.println("<tr><td>Start Time</td><td>" + flow.getStartTime() + "</td></tr>");
		// message.println("<tr><td>End Time</td><td>" + flow.getEndTime() + "</td></tr>");
		// message.println("<tr><td>Duration</td><td>" + Utils.formatDuration(flow.getStartTime(), flow.getEndTime()) + "</td></tr>");
		// message.println("</table>");
		// message.println("");
		// String executionUrl = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execId;
		// message.println("<a href=\"" + executionUrl + "\">" + flow.getFlowId() + " Execution Link</a>");
	}
}
