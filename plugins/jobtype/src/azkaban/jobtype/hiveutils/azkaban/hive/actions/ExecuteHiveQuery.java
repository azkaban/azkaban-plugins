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

package azkaban.jobtype.hiveutils.azkaban.hive.actions;

import azkaban.jobtype.hiveutils.HiveQueryExecutionException;
import azkaban.jobtype.hiveutils.HiveQueryExecutor;
import azkaban.jobtype.hiveutils.azkaban.HiveAction;
import azkaban.jobtype.hiveutils.azkaban.HiveViaAzkabanException;
import azkaban.jobtype.hiveutils.azkaban.Utils;
import azkaban.jobtype.hiveutils.util.AzkabanJobPropertyDescription;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Execute the provided Hive query. Queries can be specified to Azkaban either
 * directly or as a pointer to a file provided with workflow.
 */
public class ExecuteHiveQuery implements HiveAction {
  private final static Logger LOG = Logger
      .getLogger("com.linkedin.hive.azkaban.hive.actions.ExecuteHiveQuery");
  @AzkabanJobPropertyDescription("Verbatim query to execute. Can also specify hive.query.nn where nn is a series of padded numbers, which will be executed in order")
  public static final String HIVE_QUERY = "hive.query";
  @AzkabanJobPropertyDescription("File to load query from.  Should be in same zip.")
  public static final String HIVE_QUERY_FILE = "hive.query.file";
  @AzkabanJobPropertyDescription("URL to retrieve the query from.")
  public static final String HIVE_QUERY_URL = "hive.query.url";

  private final HiveQueryExecutor hqe;
  private final String q;

  public ExecuteHiveQuery(Properties properties, HiveQueryExecutor hqe)
      throws HiveViaAzkabanException {
    Utils.QueryPropKeys keys = new Utils.QueryPropKeys(HIVE_QUERY, HIVE_QUERY, HIVE_QUERY_FILE, HIVE_QUERY_URL);
    this. q = Utils.determineQuery(properties, keys);
    this.hqe = hqe;
  }


  @Override
  public void execute() throws HiveViaAzkabanException {
    try {
      hqe.executeQuery(q);
    } catch (HiveQueryExecutionException e) {
      throw new HiveViaAzkabanException(e);
    }
  }
}
