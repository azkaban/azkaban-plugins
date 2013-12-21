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

import org.apache.pig.tools.pigstats.OutputStats;

public class PigOutputStats {
  private OutputStats stats;

  public PigOutputStats(OutputStats stats) {
    this.stats = stats;
  }

  public Object toJson() {
    Map<String, String> jsonObj = new HashMap<String, String>();
    jsonObj.put("alias", stats.getAlias());
    jsonObj.put("bytes", Long.toString(stats.getBytes()));
    jsonObj.put("functionName", stats.getFunctionName());
    jsonObj.put("location", stats.getLocation());
    jsonObj.put("name", stats.getName());
    jsonObj.put("numberRecords", Long.toString(stats.getNumberRecords()));
    return jsonObj;
  }

  public static PigOutputStats fromJson(Object obj) {

  }
}
