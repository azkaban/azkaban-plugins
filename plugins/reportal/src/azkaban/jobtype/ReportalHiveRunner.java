package azkaban.jobtype;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

public class ReportalHiveRunner extends ReportalAbstractRunner {

	public ReportalHiveRunner(String jobName, Properties props) {
		super(props);
	}

	@Override
	protected void runReportal() throws Exception {
		System.out.println("Reportal Hive: Setting up Hive");
		HiveConf conf = new HiveConf(SessionState.class);

		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
			conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		File tempTSVFile = new File("./temp.tsv");
		OutputStream tsvTempOutputStream = new BufferedOutputStream(new FileOutputStream(tempTSVFile));
		PrintStream logOut = System.out;

		// NOTE: It is critical to do this here so that log4j is reinitialized
		// before any of the other core hive classes are loaded
		// criccomini@linkedin.com: I disabled this because it appears to swallow
		// all future logging (even outside of hive).
		// SessionState.initHiveLog4j();

		String orig = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);

		CliSessionState sessionState = new CliSessionState(conf);
		sessionState.in = System.in;
		sessionState.out = new PrintStream(tsvTempOutputStream, true, "UTF-8");
		sessionState.err = new PrintStream(logOut, true, "UTF-8");

		OptionsProcessor oproc = new OptionsProcessor();

		// Feed in Hive Args
		String[] args = buildHiveArgs();
		if (!oproc.process_stage1(args)) {
			throw new Exception("unable to parse options stage 1");
		}

		if (!oproc.process_stage2(sessionState)) {
			throw new Exception("unable to parse options stage 2");
		}

		// Set all properties specified via command line
		for (Map.Entry<Object, Object> item: sessionState.cmdProperties.entrySet()) {
			conf.set((String)item.getKey(), (String)item.getValue());
		}
		
		SessionState.start(sessionState);

		String expanded = expandHiveAuxJarsPath(orig);
		if (orig == null || orig.equals(expanded)) {
			System.out.println("Hive aux jars variable not expanded");
		} else {
			System.out.println("Expanded aux jars variable from [" + orig + "] to [" + expanded + "]");
			HiveConf.setVar(conf, HiveConf.ConfVars.HIVEAUXJARS, expanded);
		}
		
		if (!ShimLoader.getHadoopShims().usesJobShell()) {
			// hadoop-20 and above - we need to augment classpath using hiveconf
			// components
			// see also: code in ExecDriver.java
			ClassLoader loader = conf.getClassLoader();
			String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
			
			System.out.println("Got auxJars = " + auxJars);

			if (StringUtils.isNotBlank(auxJars)) {
				loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
			}
			conf.setClassLoader(loader);
			Thread.currentThread().setContextClassLoader(loader);
		}

		CliDriver cli = new CliDriver();
		int returnValue = 0;
		String prefix = "";

		returnValue = cli.processLine("set hive.cli.print.header=true;");
		String[] queries = jobQuery.split("\n");
		for (String line: queries) {
			if (!prefix.isEmpty()) {
				prefix += '\n';
			}
			if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
				line = prefix + line;
				line = injectVariables(line);
				System.out.println("Reportal Hive: Running Hive Query: " + line);
				System.out.println("Reportal Hive: HiveConf HIVEAUXJARS: " + HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS));
				returnValue = cli.processLine(line);
				prefix = "";
			}
			else {
				prefix = prefix + line;
				continue;
			}
		}

		tsvTempOutputStream.close();

		// convert tsv to csv and write it do disk
		System.out.println("Reportal Hive: Converting output");
		InputStream tsvTempInputStream = new BufferedInputStream(new FileInputStream(tempTSVFile));
		Scanner rowScanner = new Scanner(tsvTempInputStream);
		PrintStream csvOutputStream = new PrintStream(outputStream);
		while (rowScanner.hasNextLine()) {
			String tsvLine = rowScanner.nextLine();
			// strip all quotes, and then quote the columns
			csvOutputStream.println("\"" + tsvLine.replace("\"", "").replace("\t", "\",\"") + "\"");
		}
		rowScanner.close();
		csvOutputStream.close();

		// Flush the temp file out
		tempTSVFile.delete();

		if (returnValue != 0) {
			throw new Exception("Hive query finished with a non zero return code");
		}

		System.out.println("Reportal Hive: Ended successfully");
	}

	private String[] buildHiveArgs() {
		String hadoopBinDir = props.getString("hadoop.dir.bin");
		String hadoopConfDir = props.getString("hadoop.dir.conf");
		String hiveAuxJarsPath = props.getString("hive.aux.jars.path");

		List<String> confBuilder = new ArrayList<String>();

		if (proxyUser != null) {
			confBuilder.add("hive.exec.scratchdir=/tmp/hive-" + proxyUser);
		}
		if (hadoopBinDir != null) {
			confBuilder.add("hadoop.bin.path=" + hadoopBinDir);
		}
		if (hadoopConfDir != null) {
			confBuilder.add("hadoop.config.dir=" + hadoopConfDir);
		}
		if (hiveAuxJarsPath != null) {
			hiveAuxJarsPath = "file://" + hiveAuxJarsPath.replace(",", ",file://");
			confBuilder.add("hive.aux.jars.path=" + hiveAuxJarsPath);
		}
		if (jobTitle != null) {
			confBuilder.add("mapred.job.name=\"Reportal: " + jobTitle + "\"");
		}

		// if (logDir != null) {
		// confBuilder.add("hive.log.dir=" + logDir);
		// }
		// if (logFile != null) {
		// confBuilder.add("hive.log.file=" + logFile);
		// }
		// if (mapredJobQueueName != null) {
		// confBuilder.add("mapred.job.queue.name=" + mapredJobQueueName);
		// }

		String[] args = new String[confBuilder.size() * 2];

		for (int i = 0; i < confBuilder.size(); i++) {
			args[i * 2] = "--hiveconf";
			args[i * 2 + 1] = confBuilder.get(i);
		}

		return args;
	}

	/**
	 * Normally hive.aux.jars.path is expanded from just being a path to the
	 * full list of files in the directory by the hive shell script. Since
	 * we normally won't be running from the script, it's up to us to do that
	 * work here. We use a heuristic that if there is no occurrence of ".jar"
	 * in the original, it needs expansion. Otherwise it's already been done
	 * for us.
	 * Also, surround the files with uri niceities.
	 */
	static String expandHiveAuxJarsPath(String original) throws IOException {
		if (original == null || original.endsWith(".jar"))
			return original;

		File[] files = new File(original).listFiles();

		if (files == null || files.length == 0) {
			return original;
		}

		return filesToURIString(files);

	}

	static String filesToURIString(File[] files) throws IOException {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < files.length; i++) {
			sb.append("file:///").append(files[i].getCanonicalPath());
			if (i != files.length - 1)
				sb.append(",");
		}

		return sb.toString();
	}
}
