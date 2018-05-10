package azkaban.jobtype;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import azkaban.jobtype.tuning.TuningErrorHandler;


public class TestTuningErrorHandler {

  @Test
  public void testTuningErrorHandler() throws IOException {
    TuningErrorHandler tuningErrorHandler = new TuningErrorHandler();
    String message = "Error: Java heap space. Got following exception ";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorHandler.containsAutoTuningError(message));

    message = "Error: java.lang.OutOfMemoryError: unable to create new native thread by server ";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorHandler.containsAutoTuningError(message));

    message = "Initialization of all the collectors failed on cluster";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorHandler.containsAutoTuningError(message));

    message = "Container 10002 is running beyond virtual memory limits";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorHandler.containsAutoTuningError(message));

    message = "Failed to find class";
    Assert.assertFalse("TuningErrorHandler not identifying error pattern ",
        tuningErrorHandler.containsAutoTuningError(message));

  }

}
