package azkaban.jobtype;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopSecureSparkWrapper {
  private static Logger logger = Logger.getRootLogger();

  @Test
  public void testAutoLabeling() {
    Map<String, String> envs = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    envs.put(HadoopSparkJob.SPARK_NODE_LABELING_ENV_VAR, Boolean.TRUE.toString());
    envs.put(HadoopSparkJob.SPARK_AUTO_NODE_LABELING_ENV_VAR, Boolean.TRUE.toString());
    envs.put(HadoopSparkJob.SPARK_DESIRED_NODE_LABEL_ENV_VAR, "test2");
    envs.put(HadoopSparkJob.SPARK_MIN_MEM_VCORE_RATIO_ENV_VAR, "3");
    URL url = getClass().getClassLoader().getResource("spark-defaults.conf");
    envs.put(HadoopSparkJob.SPARK_PROPERTY_FILE_PATH_ENV_VAR, url.getPath());
    setEnv(envs);
    Configuration.addDefaultResource("yarn-site.xml");
    String[] argArray = new String[] {
        "--conf",
        "spark.yarn.queue=test",
        "--conf",
        "spark.yarn.executor.nodeLabelExpression=test",
        "--executor-cores",
        "2",
        "--executor-memory",
        "7G"
    };
    argArray = HadoopSecureSparkWrapper.handleNodeLabeling(argArray);
    argArray = HadoopSecureSparkWrapper.removeNullsFromArgArray(argArray);
    Assert.assertTrue(argArray.length == 6);
    Assert.assertTrue(argArray[1].contains("test2"));
  }

  @Test
  public void testDisableAutoLabeling() {
    Map<String, String> envs = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    envs.put(HadoopSparkJob.SPARK_NODE_LABELING_ENV_VAR, Boolean.TRUE.toString());
    envs.put(HadoopSparkJob.SPARK_DESIRED_NODE_LABEL_ENV_VAR, "test2");
    envs.put(HadoopSparkJob.SPARK_MIN_MEM_VCORE_RATIO_ENV_VAR, "3");
    URL url = getClass().getClassLoader().getResource("spark-defaults.conf");
    envs.put(HadoopSparkJob.SPARK_PROPERTY_FILE_PATH_ENV_VAR, url.getPath());
    setEnv(envs);
    Configuration.addDefaultResource("yarn-site.xml");
    String[] argArray = new String[] {
        "--conf",
        "spark.yarn.queue=test",
        "--conf",
        "spark.yarn.executor.nodeLabelExpression=test",
        "--executor-cores",
        "2",
        "--executor-memory",
        "7G"
    };
    argArray = HadoopSecureSparkWrapper.handleNodeLabeling(argArray);
    argArray = HadoopSecureSparkWrapper.removeNullsFromArgArray(argArray);
    Assert.assertTrue(argArray.length == 6);
    Assert.assertTrue(argArray[1].contains("test"));
  }

  @Test
  public void testLoadConfigFromPropertyFile() {
    Map<String, String> envs = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    envs.put(HadoopSparkJob.SPARK_NODE_LABELING_ENV_VAR, Boolean.TRUE.toString());
    envs.put(HadoopSparkJob.SPARK_AUTO_NODE_LABELING_ENV_VAR, Boolean.TRUE.toString());
    envs.put(HadoopSparkJob.SPARK_DESIRED_NODE_LABEL_ENV_VAR, "test2");
    envs.put(HadoopSparkJob.SPARK_MIN_MEM_VCORE_RATIO_ENV_VAR, "3");
    URL url = getClass().getClassLoader().getResource("spark-defaults.conf");
    envs.put(HadoopSparkJob.SPARK_PROPERTY_FILE_PATH_ENV_VAR, url.getPath());
    setEnv(envs);
    Configuration.addDefaultResource("yarn-site.xml");
    String[] argArray = new String[] {
        "--conf",
        "spark.yarn.queue=test",
        "--conf",
        "spark.yarn.executor.nodeLabelExpression=test"
    };
    argArray = HadoopSecureSparkWrapper.handleNodeLabeling(argArray);
    argArray = HadoopSecureSparkWrapper.removeNullsFromArgArray(argArray);
    Assert.assertTrue(argArray.length == 2);
    Assert.assertTrue(argArray[1].contains("test2"));
  }

  @SuppressWarnings("unchecked")
  private static void setEnv(Map<String, String> newenv) {
    try
    {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class<?>[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for(Class<?> cl : classes) {
          if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        logger.error(e2);
      }
    } catch (Exception e1) {
      logger.error(e1);
    }
  }
}