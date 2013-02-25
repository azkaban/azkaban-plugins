package azkaban.jobtype;

/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.security.DefaultHadoopSecurityManager;
import azkaban.security.HadoopSecurityManager;
import azkaban.security.HadoopSecurityManagerException;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

/*
 * need lib:
 * apache pig
 * hadoop-core*.jar
 * HadoopSecurePigWrapper
 * HadoopSecurityManager(corresponding version with hadoop)
 */

public class HadoopPigJob extends JavaProcessJob {

	public static final String PIG_SCRIPT = "pig.script";
	public static final String UDF_IMPORT = "udf.import.list";
	public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
	public static final String PIG_PARAM_PREFIX = "param.";
	public static final String PIG_PARAM_FILES = "paramfile";
	public static final String HADOOP_UGI = "hadoop.job.ugi";
	public static final String DEBUG = "debug";

	// should point to the specific pig installation libs
	public static String PIG_JAVA_CLASS = "org.apache.pig.Main";
	public static String HADOOP_SECURE_PIG_WRAPPER = "azkaban.jobtype.HadoopSecurePigWrapper";

	private HadoopSecurityManager hadoopSecurityManager;
	
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	
	public HadoopPigJob(String jobid, Props sysProps, Props jobProps, Logger log) {
		super(jobid, sysProps, jobProps, log);

		HADOOP_SECURE_PIG_WRAPPER = HadoopSecurePigWrapper.class.getName();
		PIG_JAVA_CLASS = org.apache.pig.Main.class.getName();
		
		if(shouldProxy(getSysProps())) {
			getLog().info("Initiating hadoop security manager.");
			try {
				hadoopSecurityManager = loadHadoopSecurityManager(sysProps);
			}
			catch(RuntimeException e) {
				throw new RuntimeException("Failed to get hadoop security manager!" + e);
			}
		}
	}

	public boolean shouldProxy(Props prop) {
		String shouldProxy = prop.getString("azkaban.should.proxy");
		return shouldProxy != null && shouldProxy.equals("true");
	}
	
	@Override
	public void run() throws Exception {
		if(shouldProxy(getSysProps())) {
			getLog().info("Need to proxy. Getting tokens.");
			getHadoopTokens(getJobProps());
		}
		try {
			super.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new Exception(e);
		}
		finally{
			if(getJobProps().containsKey("env."+UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION)) {
				File f = new File(getJobProps().getString("env."+UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION));
				if(f.exists()) {
					f.delete();
				}
			}
		}
	}
	
	private HadoopSecurityManager loadHadoopSecurityManager(Props props) throws RuntimeException {
		
		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true, HadoopPigJob.class.getClassLoader());
		getLog().info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
		HadoopSecurityManager hadoopSecurityManager = null;

		if (hadoopSecurityManagerClass != null && hadoopSecurityManagerClass.getConstructors().length > 0) {

			try {
				Constructor<?> hsmConstructor = hadoopSecurityManagerClass.getConstructor(Props.class);
				hadoopSecurityManager = (HadoopSecurityManager) hsmConstructor.newInstance(props);
			} 
			catch (InvocationTargetException e) {
				getLog().error("Could not instantiate Hadoop Security Manager "+ hadoopSecurityManagerClass.getName() + e.getCause());
				throw new RuntimeException(e.getCause());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e.getCause());
			} 
		} 
		else {
			hadoopSecurityManager = new DefaultHadoopSecurityManager();
		}

		return hadoopSecurityManager;

	}
	
	protected void getHadoopTokens(Props props) throws HadoopSecurityManagerException {

		File tokenFile = null;
		try {
			tokenFile = File.createTempFile("mr-azkaban", ".token");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new HadoopSecurityManagerException("Failed to create the token file.", e);
		}
		
		hadoopSecurityManager.prefetchToken(tokenFile, props.getString(HadoopSecurityManager.TO_PROXY));
		
		props.put("env."+UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
		
	}
	
	@Override
	protected String getJavaClass() {
		return shouldProxy(getSysProps()) ? HADOOP_SECURE_PIG_WRAPPER : PIG_JAVA_CLASS;
	}

	@Override
	protected String getJVMArguments() {
		String args = super.getJVMArguments();

		String typeGlobalJVMArgs = getSysProps().getString("jobtype.global.jvm.args", null);
		if (typeGlobalJVMArgs != null) {
			args += " " + typeGlobalJVMArgs;
		}
		
		List<String> udfImport = getUDFImportList();
		if (udfImport.size() > 0) {
			args += " -Dudf.import.list=" + super.createArguments(udfImport, ":");
		}
		
		List<String> additionalJars = getAdditionalJarsList();
		if (additionalJars.size() > 0) {
			args += " -Dpig.additional.jars=" + super.createArguments(additionalJars, ":");
		}

		String hadoopUGI = getHadoopUGI();
		if (hadoopUGI != null) {
			args += " -Dhadoop.job.ugi=" + hadoopUGI;
		}

		if(shouldProxy(getSysProps())) {
			info("Setting up secure proxy info for child process");
			String secure;
			secure = " -D" + HadoopSecurityManager.TO_PROXY + "=" + getJobProps().getString(HadoopSecurityManager.TO_PROXY);
			String extraToken = getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN);
			if(extraToken != null) {
				secure += " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "=" + extraToken;
			}
			info("Secure settings = " + secure);
			args += secure;
		} else	{
			info("Not setting up secure proxy info for child process");
		}

		return args;
	}

	@Override
	protected String getMainArguments() {
		ArrayList<String> list = new ArrayList<String>();
		Map<String, String> map = getPigParams();
		if (map != null) {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add("-param " + StringUtils.shellQuote(entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
			}
		}

		List<String> paramFiles = getPigParamFiles();
		if (paramFiles != null) {
			for (String paramFile : paramFiles) {
				list.add("-param_file " + paramFile);
			}
		}
		
		if (getDebug()) {
			list.add("-debug");
		}

		list.add(getScript());

		return StringUtils.join(list, " ");
	}

	@Override
	protected List<String> getClassPaths() {
		
		List<String> classPath = super.getClassPaths();

		// Add hadoop home setting.
//		String hadoopHome = System.getenv("HADOOP_HOME");
//		if (hadoopHome == null) {
//			info("HADOOP_HOME not set, using default hadoop config.");
//		} else {
//			info("Using hadoop config found in " + hadoopHome);
//			classPath.add(new File(hadoopHome, "conf").getPath());
//		}

//		// add pig jar
//		getLog().info("Getting pigmain from");
//		classPath.add(getSourcePathFromClass(org.apache.pig.Main.class));
//		// add hadoop jar
//		classPath.add(getSourcePathFromClass(Configuration.class));
//		
//		if(shouldProxy(getSysProps())) {
//			classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));
//			classPath.add(getSourcePathFromClass(HadoopSecurePigWrapper.class));
//		}
		classPath.add(getSourcePathFromClass(Props.class));
		classPath.add(getSourcePathFromClass(HadoopSecurePigWrapper.class));
		List<String> typeClassPath = getSysProps().getStringList("jobtype.classpath", null, ",");
		if(typeClassPath != null) {
			// fill in this when load this jobtype
			String pluginDir = getSysProps().get("plugin.dir");
			for(String jar : typeClassPath) {
				File jarFile = new File(jar);
				if(!jarFile.isAbsolute()) {
					jarFile = new File(pluginDir + File.separatorChar + jar);
				}
				
				if(!classPath.contains(jarFile.getAbsoluteFile())) {
					classPath.add(jarFile.getAbsolutePath());
				}
			}
		}
		
		
		List<String> typeGlobalClassPath = getSysProps().getStringList("jobtype.global.classpath", null, ",");
		if(typeGlobalClassPath != null) {
			for(String jar : typeGlobalClassPath) {
				if(!classPath.contains(jar)) {
					classPath.add(jar);
				}
			}
		}
		
		return classPath;
	}

	protected boolean getDebug() {
		return getJobProps().getBoolean(DEBUG, false);
	}

	protected String getScript() {
		return getJobProps().getString(PIG_SCRIPT);
	}

	protected List<String> getUDFImportList() {
		List<String> udfImports = new ArrayList<String>();
		List<String> typeImports = getSysProps().getStringList(UDF_IMPORT, null, ",");
		List<String> jobImports = getJobProps().getStringList(UDF_IMPORT, null, ",");
		if(typeImports != null) {
			udfImports.addAll(typeImports);
		}
		if(jobImports != null) {
			udfImports.addAll(jobImports);
		}
		return udfImports;
	}

	protected List<String> getAdditionalJarsList() {
		List<String> additionalJars = new ArrayList<String>();
		List<String> typeJars = getSysProps().getStringList(PIG_ADDITIONAL_JARS, null, ",");
		List<String> jobJars = getJobProps().getStringList(PIG_ADDITIONAL_JARS, null, ",");
		if(typeJars != null) {
			additionalJars.addAll(typeJars);
		}
		if(jobJars != null) {
			additionalJars.addAll(jobJars);
		}
		return additionalJars;
	}
	
	protected String getHadoopUGI() {
		return getJobProps().getString(HADOOP_UGI, null);
	}
	
	protected Map<String, String> getPigParams() {
		return getJobProps().getMapByPrefix(PIG_PARAM_PREFIX);
	}

	protected List<String> getPigParamFiles() {
		return getJobProps().getStringList(PIG_PARAM_FILES, null, ",");
	}
	
	
	private static String getSourcePathFromClass(Class<?> containedClass) {
		File file = new File(containedClass.getProtectionDomain().getCodeSource().getLocation().getPath());
		
		if (!file.isDirectory() && file.getName().endsWith(".class")) {
			String name = containedClass.getName();
			StringTokenizer tokenizer = new StringTokenizer(name, ".");
			while (tokenizer.hasMoreTokens()) {
				tokenizer.nextElement();
				file = file.getParentFile();
			}

			return file.getPath();
		} else {
			return containedClass.getProtectionDomain().getCodeSource()
					.getLocation().getPath();
		}
	}
}

