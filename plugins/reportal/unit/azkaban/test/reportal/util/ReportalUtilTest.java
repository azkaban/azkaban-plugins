/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.test.reportal.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.reportal.util.ReportalUtil;

public class ReportalUtilTest {

  @Test
  public void testSortExecutableNodesNullFlow() {
    List<ExecutableNode> sortedNodes = ReportalUtil.sortExecutableNodes(null);
    Assert.assertTrue("sortedNodes was not empty but was expected to be.",
        sortedNodes.isEmpty());
  }

  @Test
  public void testSortExecutableNodesEmptyFlow() {
    ExecutableFlow flow = new ExecutableFlow();
    List<ExecutableNode> sortedNodes = ReportalUtil.sortExecutableNodes(flow);
    Assert.assertTrue("sortedNodes was not empty but was expected to be.",
        sortedNodes.isEmpty());
  }

  @Test
  public void testSortExecutableNodesSingleNodeFlow() {
    Node node = new Node("node1");
    node.setType("jobType");

    List<Node> nodes = new ArrayList<Node>();
    nodes.add(node);

    ExecutableFlow executableFlow = createExecutableFlow(nodes);
    List<ExecutableNode> sortedNodes =
        ReportalUtil.sortExecutableNodes(executableFlow);
    Assert.assertEquals("sortedNodes was expected to have 1 node but had "
        + sortedNodes.size(), sortedNodes.size(), 1);
  }

  @Test
  public void testSortExecutableNodesMultipleNodeFlow() {
    final int SIZE = 5;
    List<Node> nodes = new ArrayList<Node>(SIZE);

    for (int i = 0; i < SIZE; i++) {
      Node node = new Node("node" + i);
      node.setType("jobType");
      nodes.add(node);
    }

    ExecutableFlow executableFlow = createExecutableFlow(nodes);
    List<ExecutableNode> sortedNodes =
        ReportalUtil.sortExecutableNodes(executableFlow);
    Assert.assertEquals("sortedNodes was expected to have " + SIZE
        + " nodes but had " + sortedNodes.size(), sortedNodes.size(), SIZE);

    for (int i = 0; i < SIZE; i++) {
      Assert.assertTrue("Node was expected to have an id of \"node" + i
          + "\" but instead had " + "an id of \"" + sortedNodes.get(i).getId()
          + "\"", sortedNodes.get(i).getId().equals("node" + i));
    }
  }

  /**
   * Creates a linear executable flow with the specified nodes.
   *
   * @param nodes
   * @return
   */
  private ExecutableFlow createExecutableFlow(List<Node> nodes) {
    if (nodes == null || nodes.size() == 0) {
      return new ExecutableFlow();
    }

    Flow flow = new Flow("flow1");
    Node prevNode = nodes.get(0);
    flow.addNode(prevNode);

    for (int i = 1; i < nodes.size(); i++) {
      Node nextNode = nodes.get(i);
      flow.addNode(nextNode);

      Edge edge = new Edge(prevNode.getId(), nextNode.getId());
      flow.addEdge(edge);

      prevNode = nextNode;
    }

    flow.initialize();

    Project project = new Project(1, "project1");

    return new ExecutableFlow(project, flow);
  }

}
