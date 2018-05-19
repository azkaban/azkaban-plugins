/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.jobtype;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;

import azkaban.jobtype.tuning.TuningParameterUtils;
import azkaban.utils.Props;


public class TestTuningParameterUtils {

  @Test
  public void testGetHadoopProperties() throws IOException {
    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");

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
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");

    String defaultParamJSON = TuningParameterUtils.getDefaultJobParameterJSON(props);
    String expectedJSON =
        "{\"mapreduce.task.io.sort.mb\":\"100\",\"mapreduce.map.memory.mb\":\"2048\",\"mapreduce.reduce.memory.mb\":\"2048\",\"mapreduce.task.io.sort.factor\":\"100\"}";
    Assert.assertEquals("Wrong JSON return for default job parameter ", defaultParamJSON, expectedJSON);
  }
}
