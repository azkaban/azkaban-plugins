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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;

public class ReportalUtil {
  public static IStreamProvider getStreamProvider(String fileSystem) {
    if (fileSystem.equalsIgnoreCase("hdfs")) {
      return new StreamProviderHDFS();
    }
    return new StreamProviderLocal();
  }

  /**
   * Returns a list of the executable nodes in the specified flow in execution
   * order. Assumes that the flow is linear.
   *
   * @param nodes
   * @return
   */
  public static List<ExecutableNode> sortExecutableNodes(ExecutableFlow flow) {
    List<ExecutableNode> sortedNodes = new ArrayList<ExecutableNode>();

    if (flow != null) {
      List<String> startNodeIds = flow.getStartNodes();

      String nextNodeId = startNodeIds.isEmpty() ? null : startNodeIds.get(0);

      while (nextNodeId != null) {
        ExecutableNode node = flow.getExecutableNode(nextNodeId);
        sortedNodes.add(node);

        Set<String> outNodes = node.getOutNodes();
        nextNodeId = outNodes.isEmpty() ? null : outNodes.iterator().next();
      }
    }

    return sortedNodes;
  }
}
