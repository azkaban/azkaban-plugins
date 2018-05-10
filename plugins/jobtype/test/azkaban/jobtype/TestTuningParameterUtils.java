package azkaban.jobtype;

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import azkaban.jobtype.tuning.TuningParameterUtils;
import azkaban.utils.Props;


public class TestTuningParameterUtils {

  @Test
  public void testGetHadoopProperties() throws IOException {
    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.mb", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.factor", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.reduce.memory.mb", "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.map.memory.mb", "2048");
    Map<String, String> confProperties = TuningParameterUtils.getHadoopProperties(props);
    Assert.assertTrue("Failed to get hadoop properties mapreduce.task.io.sort.mb ",
        confProperties.containsKey("mapreduce.task.io.sort.mb"));
    Assert.assertTrue("Failed to get right value for hadoop properties mapreduce.task.io.sort.mb ",
        confProperties.get("mapreduce.task.io.sort.mb").equals("100"));

    Assert.assertTrue("Failed to get hadoop properties mapreduce.task.io.sort.factor ",
        confProperties.containsKey("mapreduce.task.io.sort.factor"));
    Assert.assertTrue("Failed to get right value for hadoop properties mapreduce.task.io.sort.factor ", confProperties
        .get("mapreduce.task.io.sort.factor").equals("100"));

    Assert.assertTrue("Failed to get hadoop properties mapreduce.reduce.memory.mb ",
        confProperties.containsKey("mapreduce.reduce.memory.mb"));
    Assert.assertTrue("Failed to get right value for hadoop properties mapreduce.reduce.memory.mb ", confProperties
        .get("mapreduce.reduce.memory.mb").equals("2048"));

    Assert.assertTrue("Failed to get hadoop properties mapreduce.map.memory.mb ",
        confProperties.containsKey("mapreduce.map.memory.mb"));
    Assert.assertTrue("Failed to get right value for hadoop properties mapreduce.map.memory.mb ",
        confProperties.get("mapreduce.map.memory.mb").equals("2048"));
  }

  @Test
  public void testGetDefaultJobParameterJSON() throws IOException {
    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.mb", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.task.io.sort.factor", "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.reduce.memory.mb", "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + "mapreduce.map.memory.mb", "2048");
    String defaultParamJSON = TuningParameterUtils.getDefaultJobParameterJSON(props);
    String expectedJSON =
        "{\"mapreduce.task.io.sort.mb\":\"100\",\"mapreduce.map.memory.mb\":\"2048\",\"mapreduce.reduce.memory.mb\":\"2048\",\"mapreduce.task.io.sort.factor\":\"100\"}";
    Assert.assertEquals("Wrong JSON return for default job parameter ", defaultParamJSON, expectedJSON);
  }
}
