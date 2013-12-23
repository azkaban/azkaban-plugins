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

package azkaban.reportal.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.apache.commons.lang.StringUtils;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;

public class ReportalHelper {
	public static List<Project> getReportalProjects(AzkabanWebServer server) {
		List<Project> projects = server.getProjectManager().getProjects();

		List<Project> reportalProjects = new ArrayList<Project>();

		for (Project project : projects) {
			if (project.getMetadata().containsKey("reportal-user")) {
				reportalProjects.add(project);
			}
		}

		return reportalProjects;
	}

	public static List<Project> getReportalProjectsWithFilter(AzkabanWebServer server) {
		List<Project> projects = server.getProjectManager().getProjects();

		List<Project> reportalProjects = new ArrayList<Project>();

		for (Project project : projects) {
			if (project.getMetadata().containsKey("reportal-user")) {
				// Apply filter
				reportalProjects.add(project);
			}
		}

		return reportalProjects;
	}

	public static void bookmarkProject(AzkabanWebServer server, Project project, User user) throws ProjectManagerException {
		project.getMetadata().put("bookmark-" + user.getUserId(), true);
		server.getProjectManager().updateProjectSetting(project);
	}

	public static void unBookmarkProject(AzkabanWebServer server, Project project, User user) throws ProjectManagerException {
		project.getMetadata().remove("bookmark-" + user.getUserId());
		server.getProjectManager().updateProjectSetting(project);
	}

	public static boolean isBookmarkProject(Project project, User user) {
		return project.getMetadata().containsKey("bookmark-" + user.getUserId());
	}

	public static void subscribeProject(AzkabanWebServer server, Project project, User user, String email) throws ProjectManagerException {
		@SuppressWarnings("unchecked")
		Map<String, String> subscription = (Map<String, String>) project.getMetadata().get("subscription");
		if (subscription == null) {
			subscription = new HashMap<String, String>();
		}
		
		if (email != null && !email.isEmpty()) {
			subscription.put(user.getUserId(), email);
		}
		
		project.getMetadata().put("subscription", subscription);
		updateProjectNotifications(project, server.getProjectManager());
		server.getProjectManager().updateProjectSetting(project);
	}

	public static void unSubscribeProject(AzkabanWebServer server, Project project, User user) throws ProjectManagerException {
		@SuppressWarnings("unchecked")
		Map<String, String> subscription = (Map<String, String>) project.getMetadata().get("subscription");
		if (subscription == null) {
			return;
		}
		subscription.remove(user.getUserId());
		project.getMetadata().put("subscription", subscription);
		updateProjectNotifications(project, server.getProjectManager());
		server.getProjectManager().updateProjectSetting(project);
	}

	public static boolean isSubscribeProject(Project project, User user) {
		@SuppressWarnings("unchecked")
		Map<String, String> subscription = (Map<String, String>) project.getMetadata().get("subscription");
		if (subscription == null) {
			return false;
		}
		return subscription.containsKey(user.getUserId());
	}

	public static void updateProjectNotifications(Project project, ProjectManager pm) throws ProjectManagerException {
		Flow flow = project.getFlows().get(0);
		ArrayList<String> emails = new ArrayList<String>();
		String notifications = (String) project.getMetadata().get("notifications");
		String[] emailSplit = notifications.split("\\s*,\\s*|\\s*;\\s*|\\s+");
		emails.addAll(Arrays.asList(emailSplit));
		// Add notification emails
		@SuppressWarnings("unchecked")
		Map<String, String> subscription = (Map<String, String>) project.getMetadata().get("subscription");
		if (subscription != null) {
			emails.addAll(subscription.values());
		}
		ArrayList<String> emailList = new ArrayList<String>();
		for (String email : emails) {
			email = email.trim();
			if (!email.isEmpty()) {
				emailList.add(email);
			}
		}
		Object ownerEmail = project.getMetadata().get("owner-email");
		if(ownerEmail != null && !ownerEmail.toString().trim().isEmpty()) {
			emailList.add(ownerEmail.toString().trim());
		}
		// Put notifications on the flow
		flow.getSuccessEmails().clear();
		flow.getFailureEmails().clear();
		flow.addSuccessEmails(emailList);
		flow.addFailureEmails(emailList);
		pm.updateFlow(project, flow);
	}

	public static boolean isScheduledProject(Project project) {
		Object schedule = project.getMetadata().get("schedule");
		if (schedule == null || !(schedule instanceof Boolean)) {
			return false;
		}
		return (boolean) (Boolean) schedule;
	}

	public static boolean isScheduleEnabledProject(Project project) {
		Object schedule = project.getMetadata().get("scheduleEnabled");
		if (schedule == null || !(schedule instanceof Boolean)) {
			return false;
		}
		return (boolean) (Boolean) schedule;
	}

	public static List<Project> getUserReportalProjects(AzkabanWebServer server, String userName) throws ProjectManagerException {
		ProjectManager projectManager = server.getProjectManager();
		List<Project> projects = projectManager.getProjects();
		List<Project> result = new ArrayList<Project>();

		for (Project project : projects) {
			if (userName.equals(project.getMetadata().get("reportal-user"))) {
				result.add(project);
			}
		}

		return result;
	}

	public static Project createReportalProject(AzkabanWebServer server, String title, String description, User user) throws ProjectManagerException {
		ProjectManager projectManager = server.getProjectManager();
		String projectName = "reportal-" + user.getUserId() + "-" + sanitizeText(title);
		Project project = projectManager.getProject(projectName);
		if (project != null) {
			return null;
		}
		project = projectManager.createProject(projectName, description, user);

		return project;
	}

	public static String sanitizeText(String text) {
		return text.replaceAll("[^A-Za-z0-9]", "-");
	}

	public static File findAvailableFileName(File parent, String name, String extension) {
		if (name.isEmpty()) {
			name = "untitled";
		}
		File file = new File(parent, name + extension);
		int i = 1;
		while (file.exists()) {
			file = new File(parent, name + "-" + i + extension);
			i++;
		}
		return file;
	}

	public static String prepareStringForJS(Object object) {
		return object.toString().replace("\r", "").replace("\n", "\\n");
	}

	public static String[] filterCSVFile(String[] files) {
		List<String> result = new ArrayList<String>();
		for (int i = 0; i < files.length; i++) {
			if (StringUtils.endsWithIgnoreCase(files[i], ".csv")) {
				result.add(files[i]);
			}
		}
		return result.toArray(new String[result.size()]);
	}

	public static boolean isValidEmailAddress(String email) {
		boolean result = true;
		try {
			InternetAddress emailAddr = new InternetAddress(email);
			emailAddr.validate();
		} catch (AddressException ex) {
			result = false;
		}
		return result;
	}
}
