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

package azkaban.reportal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import azkaban.executor.ExecutableNode;

public class ReportalUtil {
	public static IStreamProvider getStreamProvider(String fileSystem) {
		if (fileSystem.equalsIgnoreCase("hdfs")) {
			return new StreamProviderHDFS();
		}
		return new StreamProviderLocal();
	}
	
	/**
	 * Returns a new list with the nodes sorted in execution order.
	 * @param nodes
	 * @return
	 */
	public static List<ExecutableNode> sortExecutableNodes(List<ExecutableNode> nodes) {
	  List<ExecutableNode> sortedNodes = new ArrayList<ExecutableNode>();
    
	  String prevNodeId = null;
    for (int i = 0; i < nodes.size(); i++) {
      for (int j = 0; j < nodes.size(); j++) {
        ExecutableNode job = nodes.get(j);
        
        Set<String> inNodes = job.getInNodes();
        String inNodeId = inNodes.size() == 0 ? null : inNodes.iterator().next();
        if ((inNodeId == prevNodeId) || (inNodeId != null && inNodeId.equals(prevNodeId))) {
          sortedNodes.add(job);
          prevNodeId = job.getId();
          break;
        }
      }
    }
    
    return sortedNodes;
	}
}
