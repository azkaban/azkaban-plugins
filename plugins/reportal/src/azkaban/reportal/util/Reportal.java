package azkaban.reportal.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Weeks;
import org.joda.time.Years;

import azkaban.executor.ExecutionOptions;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Utils;
import azkaban.viewer.reportal.ReportalMailCreator;
import azkaban.viewer.reportal.ReportalTypeManager;

public class Reportal {

	public String reportalUser;
	public String ownerEmail;

	public String title;
	public String description;

	public List<Query> queries;
	public List<Variable> variables;

	public boolean schedule;
	public boolean scheduleEnabled;
	public String scheduleInterval;
	public int scheduleTime;

	public String accessViewer;
	public String accessExecutor;
	public String accessOwner;

	public String notifications;

	public Project project;

	public void saveToProject(Project project) {
		this.project = project;

		project.getMetadata().put("reportal-user", reportalUser);
		project.getMetadata().put("owner-email", ownerEmail);

		project.getMetadata().put("title", title);
		project.setDescription(description);

		project.getMetadata().put("schedule", schedule);
		project.getMetadata().put("scheduleEnabled", scheduleEnabled);
		project.getMetadata().put("scheduleInterval", scheduleInterval);
		project.getMetadata().put("scheduleTime", scheduleTime);

		project.getMetadata().put("accessViewer", accessViewer);
		project.getMetadata().put("accessExecutor", accessExecutor);
		project.getMetadata().put("accessOwner", accessOwner);

		project.getMetadata().put("queryNumber", queries.size());
		for (int i = 0; i < queries.size(); i++) {
			Query query = queries.get(i);
			project.getMetadata().put("query" + i + "title", query.title);
			project.getMetadata().put("query" + i + "type", query.type);
			project.getMetadata().put("query" + i + "script", query.script);
		}

		project.getMetadata().put("variableNumber", variables.size());
		for (int i = 0; i < variables.size(); i++) {
			Variable variable = variables.get(i);
			project.getMetadata().put("variable" + i + "title", variable.title);
			project.getMetadata().put("variable" + i + "name", variable.name);
		}

		project.getMetadata().put("notifications", notifications);
	}

	public void removeSchedules(ScheduleManager scheduleManager) {
		List<Flow> flows = project.getFlows();
		for (Flow flow: flows) {
			Set<Schedule> schedules = scheduleManager.getSchedules(project.getId(), flow.getId());
			if (schedules != null) {
				for (Schedule schedule: schedules) {
					scheduleManager.removeSchedule(schedule);
				}
			}
		}
	}

	public void updateSchedules(ScheduleManager scheduleManager, User user, Flow flow) {
		// Clear previous schedules
		removeSchedules(scheduleManager);
		// Add new schedule
		if (schedule && scheduleEnabled) {
			DateTime firstSchedTime = new LocalDateTime().toDateTime();
			firstSchedTime = firstSchedTime.withHourOfDay(scheduleTime).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
			ReadablePeriod period = Years.ONE;
			if (scheduleInterval.equals("q")) {
				period = Months.FOUR;
			}
			else if (scheduleInterval.equals("m")) {
				period = Months.ONE;
			}
			else if (scheduleInterval.equals("w")) {
				period = Weeks.ONE;
			}
			else if (scheduleInterval.equals("d")) {
				period = Days.ONE;
			}
			else if (scheduleInterval.equals("h")) {
				period = Hours.ONE;
			}
			ExecutionOptions options = new ExecutionOptions();
			options.getFlowParameters().put("reportal.execution.user", user.getUserId());
			options.setMailCreator(ReportalMailCreator.REPORTAL_MAIL_CREATOR);

			scheduleManager.scheduleFlow(-1, project.getId(), project.getName(), flow.getId(), "ready",
					firstSchedTime.getMillis(), firstSchedTime.getZone(), period, DateTime.now().getMillis(),
					firstSchedTime.getMillis(), firstSchedTime.getMillis(), user.getUserId(), options, null);
		}
	}

	public void updatePermissions() {
		// Save permissions
		String[] accessViewerList = accessViewer.trim().split("\\s*,\\s*|\\s*;\\s*|\\s+");
		String[] accessExecutorList = accessExecutor.trim().split("\\s*,\\s*|\\s*;\\s*|\\s+");
		String[] accessOwnerList = accessOwner.trim().split("\\s*,\\s*|\\s*;\\s*|\\s+");
		// Prepare permission types
		Permission admin = new Permission();
		admin.addPermission(Type.READ);
		admin.addPermission(Type.EXECUTE);
		admin.addPermission(Type.ADMIN);
		Permission executor = new Permission();
		executor.addPermission(Type.READ);
		executor.addPermission(Type.EXECUTE);
		Permission viewer = new Permission();
		viewer.addPermission(Type.READ);
		// Sets the permissions
		project.clearUserPermission();
		for (String user: accessViewerList) {
			user = user.trim();
			if (!user.isEmpty()) {
				project.setUserPermission(user, viewer);
			}
		}
		for (String user: accessExecutorList) {
			user = user.trim();
			if (!user.isEmpty()) {
				project.setUserPermission(user, executor);
			}
		}
		for (String user: accessOwnerList) {
			user = user.trim();
			if (!user.isEmpty()) {
				project.setUserPermission(user, admin);
			}
		}
		project.setUserPermission(reportalUser, admin);
	}

	public void createZipAndUpload(ProjectManager projectManager, User user, String reportalStorageUser) throws Exception {
		// Create temp folder to make the zip file for upload
		File tempDir = Utils.createTempDir();
		File dataDir = new File(tempDir, "data");
		dataDir.mkdirs();

		// Create all job files
		String dependentJob = null;
		List<String> jobs = new ArrayList<String>();
		for (Query query: queries) {
			// Create .job file
			File jobFile = ReportalHelper.findAvailableFileName(dataDir, ReportalHelper.sanitizeText(query.title), ".job");

			String fileName = jobFile.getName();
			String jobName = fileName.substring(0, fileName.length() - 4);
			jobs.add(jobName);

			// Populate the job file
			ReportalTypeManager.createJobAndFiles(this, jobFile, jobName, query.title, query.type, query.script, dependentJob, reportalUser, null);

			// For dependency of next query
			dependentJob = jobName;
		}

		// Create the data collector job
		if (dependentJob != null) {
			String jobName = "data-collector";

			// Create .job file
			File jobFile = ReportalHelper.findAvailableFileName(dataDir, ReportalHelper.sanitizeText(jobName), ".job");
			Map<String, String> extras = new HashMap<String, String>();
			extras.put("reportal.job.number", Integer.toString(jobs.size()));
			for (int i = 0; i < jobs.size(); i++) {
				extras.put("reportal.job." + i, jobs.get(i));
			}
			ReportalTypeManager.createJobAndFiles(this, jobFile, jobName, "", ReportalTypeManager.DATA_COLLECTOR_JOB, "", dependentJob, reportalStorageUser, extras);
		}

		// Zip jobs together
		File archiveFile = new File(tempDir, project.getName() + ".zip");
		Utils.zipFolderContent(dataDir, archiveFile);

		// Upload zip
		projectManager.uploadProject(project, archiveFile, "zip", user);

		// Empty temp
		if (tempDir.exists()) {
			FileUtils.deleteDirectory(tempDir);
		}
	}

	public void loadImmutableFromProject(Project project) {
		reportalUser = stringGetter.get(project.getMetadata().get("reportal-user"));
		ownerEmail = stringGetter.get(project.getMetadata().get("owner-email"));
	}
	
	public static Reportal loadFromProject(Project project) {
		if (project == null)
		{
			return null;
		}

		Reportal reportal = new Reportal();
		Map<String, Object> metadata = project.getMetadata();
		
		reportal.loadImmutableFromProject(project);

		if (reportal.reportalUser == null || reportal.reportalUser.isEmpty()) {
			return null;
		}

		reportal.title = stringGetter.get(metadata.get("title"));
		reportal.description = project.getDescription();
		int queries = intGetter.get(project.getMetadata().get("queryNumber"));
		int variables = intGetter.get(project.getMetadata().get("variableNumber"));

		reportal.schedule = boolGetter.get(project.getMetadata().get("schedule"));
		reportal.scheduleEnabled = boolGetter.get(project.getMetadata().get("scheduleEnabled"));
		reportal.scheduleInterval = stringGetter.get(project.getMetadata().get("scheduleInterval"));
		reportal.scheduleTime = intGetter.get(project.getMetadata().get("scheduleTime"));

		reportal.accessViewer = stringGetter.get(project.getMetadata().get("accessViewer"));
		reportal.accessExecutor = stringGetter.get(project.getMetadata().get("accessExecutor"));
		reportal.accessOwner = stringGetter.get(project.getMetadata().get("accessOwner"));

		reportal.notifications = stringGetter.get(project.getMetadata().get("notifications"));

		reportal.queries = new ArrayList<Query>();

		for (int i = 0; i < queries; i++) {
			Query query = new Query();
			reportal.queries.add(query);
			query.title = stringGetter.get(project.getMetadata().get("query" + i + "title"));
			query.type = stringGetter.get(project.getMetadata().get("query" + i + "type"));
			query.script = stringGetter.get(project.getMetadata().get("query" + i + "script"));
		}

		reportal.variables = new ArrayList<Variable>();

		for (int i = 0; i < variables; i++) {
			Variable variable = new Variable();
			reportal.variables.add(variable);
			variable.title = stringGetter.get(project.getMetadata().get("variable" + i + "title"));
			variable.name = stringGetter.get(project.getMetadata().get("variable" + i + "name"));
		}

		reportal.project = project;

		return reportal;
	}

	public static Getter<Boolean> boolGetter = new Getter<Boolean>(false, Boolean.class);
	public static Getter<Integer> intGetter = new Getter<Integer>(0, Integer.class);
	public static Getter<String> stringGetter = new Getter<String>("", String.class);

	public static class Getter<T> {
		Class<?> cls;
		T defaultValue;

		public Getter(T defaultValue, Class<?> cls) {
			this.cls = cls;
			this.defaultValue = defaultValue;
		}

		@SuppressWarnings("unchecked")
		public T get(Object object) {
			if (object == null || !(cls.isAssignableFrom(object.getClass()))) {
				return defaultValue;
			}
			return (T)object;
		}
	}

	public static class Query {
		public String title;
		public String type;
		public String script;

		public String getTitle() {
			return title;
		}

		public String getType() {
			return type;
		}

		public String getScript() {
			return script;
		}
	}

	public static class Variable {
		public String title;
		public String name;

		public String getTitle() {
			return title;
		}

		public String getName() {
			return name;
		}
	}
}
