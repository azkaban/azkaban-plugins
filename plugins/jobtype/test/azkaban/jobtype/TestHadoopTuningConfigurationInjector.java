package azkaban.jobtype;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import com.google.common.io.Files;


public class TestHadoopTuningConfigurationInjector {

  @Test
  public void testTuningErrorHandler() throws IOException {

    File root = Files.createTempDir();

    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.mb", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.factor", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.reduce.memory.mb", "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.map.memory.mb", "2048");

    props.put(CommonJobProperties.EXEC_ID, CommonJobProperties.EXEC_ID);
    props.put(CommonJobProperties.FLOW_ID, CommonJobProperties.FLOW_ID);
    props.put(CommonJobProperties.JOB_ID, CommonJobProperties.JOB_ID);
    props.put(CommonJobProperties.PROJECT_NAME, CommonJobProperties.PROJECT_NAME);
    props.put(CommonJobProperties.PROJECT_VERSION, CommonJobProperties.PROJECT_VERSION);
    props.put(CommonJobProperties.EXECUTION_LINK, CommonJobProperties.EXECUTION_LINK);
    props.put(CommonJobProperties.JOB_LINK, CommonJobProperties.JOB_LINK);
    props.put(CommonJobProperties.WORKFLOW_LINK, CommonJobProperties.WORKFLOW_LINK);
    props.put(CommonJobProperties.JOBEXEC_LINK, CommonJobProperties.JOBEXEC_LINK);
    props.put(CommonJobProperties.ATTEMPT_LINK, CommonJobProperties.ATTEMPT_LINK);
    props.put(CommonJobProperties.OUT_NODES, CommonJobProperties.OUT_NODES);
    props.put(CommonJobProperties.IN_NODES, CommonJobProperties.IN_NODES);
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_DATE, CommonJobProperties.PROJECT_LAST_CHANGED_DATE);
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_BY, CommonJobProperties.PROJECT_LAST_CHANGED_BY);
    props.put(CommonJobProperties.SUBMIT_USER, CommonJobProperties.SUBMIT_USER);

    HadoopTuningConfigurationInjector.prepareResourcesToInject(props, root.getAbsolutePath());
    HadoopTuningConfigurationInjector.injectResources(props, root.getAbsolutePath());

    Configuration configuration = new Configuration(true);
    configuration.addResource(new Path(HadoopTuningConfigurationInjector.getPath(props, root.getAbsolutePath()) + "/"
        + HadoopTuningConfigurationInjector.INJECT_TUNING_FILE));
    Assert.assertEquals("Value not matching in config ", configuration.get(CommonJobProperties.EXEC_ID),
        CommonJobProperties.EXEC_ID);
    Assert.assertEquals("Value not matching in config ", configuration.get(CommonJobProperties.FLOW_ID),
        CommonJobProperties.FLOW_ID);
    Assert.assertEquals("Value not matching in config ", configuration.get(CommonJobProperties.JOB_ID),
        CommonJobProperties.JOB_ID);
    Assert.assertEquals("Value not matching in config ", configuration.get(CommonJobProperties.PROJECT_NAME),
        CommonJobProperties.PROJECT_NAME);
    Assert.assertEquals("Value not matching in config ", configuration.get(CommonJobProperties.PROJECT_VERSION),
        CommonJobProperties.PROJECT_VERSION);
    Assert.assertEquals("Value not matching in config ",
        configuration.get(CommonJobProperties.PROJECT_LAST_CHANGED_BY), CommonJobProperties.PROJECT_LAST_CHANGED_BY);

  }

}
