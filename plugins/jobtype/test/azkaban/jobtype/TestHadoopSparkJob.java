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

package azkaban.jobtype;

import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestHadoopSparkJob {
  Logger logger = Logger.getRootLogger();
  String workingDirString = "/tmp/TestHadoopSpark";
  String spark163Home = "spark-bin-x_1630";
  String spark210Home = "spark-bin-x_2105";
  String sparkDefault = "spark_default";
  String sparkHomeConf = "conf";
  String sparkDefaultFileName = "spark-defaults.conf";
  String sparkHomeLib = "lib";

  @Before
  public void beforeMethod()
      throws IOException {
    File workingDirFile = new File(workingDirString);

    if (workingDirFile.exists()) {
      FileUtils.deleteDirectory(workingDirFile);
    }
    workingDirFile.mkdirs();
    File sparkDir = new File(workingDirFile, spark163Home);
    sparkDir.mkdir();
    File sparkVerLibDir = new File(sparkDir, sparkHomeLib);
    sparkVerLibDir.mkdir();
    File sparkVerConfDir = new File(sparkDir, sparkHomeConf);
    sparkVerConfDir.mkdir();
    File sparkVerDefaultFile = new File(sparkVerConfDir, sparkDefaultFileName);
    sparkVerDefaultFile.createNewFile();

    sparkDir = new File(workingDirFile, spark210Home);
    sparkDir.mkdir();
    sparkVerLibDir = new File(sparkDir, sparkHomeLib);
    sparkVerLibDir.mkdir();
    sparkVerConfDir = new File(sparkDir, sparkHomeConf);
    sparkVerConfDir.mkdir();
    sparkVerDefaultFile = new File(sparkVerConfDir, sparkDefaultFileName);
    sparkVerDefaultFile.createNewFile();

    File sparkDefaultDir = new File(workingDirFile, sparkDefault);
    sparkDefaultDir.mkdir();
    File sparkDefaultLibDir = new File(sparkDefaultDir, sparkHomeLib);
    sparkDefaultLibDir.mkdir();
    File sparkDefaultConfDir = new File(sparkDefaultDir, sparkHomeConf);
    sparkDefaultConfDir.mkdir();
    File sparkDefaultFile = new File(sparkDefaultConfDir, sparkDefaultFileName);
    sparkDefaultFile.createNewFile();
  }

  @Test
  public void testSparkLibConf() {
    // This method is testing whether correct spark home is selected when spark.{xyz_version}.home is specified and
    // spark-version is set to xyz_version.
    Props jobProps = new Props();
    jobProps.put(SparkJobArg.SPARK_VERSION.azPropName, "1.6.3");

    Props sysProps = new Props();
    sysProps.put("spark.home", workingDirString + "/" + sparkDefault);
    sysProps.put("spark.1.6.3.home", workingDirString + "/" + spark163Home);

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = workingDirString + "/" + spark163Home + "/" + sparkHomeLib;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = workingDirString + "/" + spark163Home + "/" + sparkHomeConf;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }

  @Test
  public void testSparkDefaultLibConf() {
    // This method is testing whether correct spark home is selected when spark-version is not set in job. In this case,
    // default spark home will be picked up.
    Props jobProps = new Props();

    Props sysProps = new Props();
    sysProps.put("spark.home", workingDirString + "/" + sparkDefault);
    sysProps.put("spark.1.6.3.home", workingDirString + "/" + spark163Home);

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = workingDirString + "/" + sparkDefault + "/" + sparkHomeLib;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = workingDirString + "/" + sparkDefault + "/" + sparkHomeConf;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }

  @Test
  public void testSparkPatternLibConf() {
    // This method is testing whether correct spark home is selected when spark.home.dir, spark.home.prefix,
    // spark.version.regex.to.replace and spark.version.regex.to.replace.with are provided and spark-version is passed
    // for which spark.{version}.home doesn't exist.
    Props jobProps = new Props();
    jobProps.put(SparkJobArg.SPARK_VERSION.azPropName, "2.1.0");

    Props sysProps = new Props();
    sysProps.put("spark.home", workingDirString + "/" + sparkDefault);
    sysProps.put("spark.1.6.3.home", workingDirString + "/" + spark163Home);
    sysProps.put("spark.home.dir", workingDirString);
    sysProps.put("spark.home.prefix", "spark-bin-x_");
    sysProps.put("spark.version.regex.to.replace", ".");
    sysProps.put("spark.version.regex.to.replace.with", "");

    HadoopSparkJob sparkJob = new HadoopSparkJob("azkaban_job_1", sysProps, jobProps, logger);
    String[] sparkHomeConfPath = sparkJob.getSparkLibConf();
    Assert.assertTrue(sparkHomeConfPath.length == 2);
    String sparkLibPath = workingDirString + "/" + spark210Home + "/" + sparkHomeLib;
    Assert.assertEquals(sparkHomeConfPath[0], sparkLibPath);
    String sparkConfPath = workingDirString + "/" + spark210Home + "/" + sparkHomeConf;
    Assert.assertEquals(sparkHomeConfPath[1], sparkConfPath);
  }
}
