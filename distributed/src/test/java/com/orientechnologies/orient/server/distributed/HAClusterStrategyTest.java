/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.server.distributed.impl.OLocalClusterWrapperStrategy;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class HAClusterStrategyTest extends AbstractHARemoveNode {
  final static int SERVERS = 2;

  @Test
  public void test() throws Exception {
    useTransactions = false;
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {
    final OrientGraphFactory factory = new OrientGraphFactory(getDatabaseURL(serverInstance.get(0)));

    final OrientGraphNoTx g = factory.getNoTx();
    g.createVertexType("Test");
    g.shutdown();

    for (int i = 0; i < 10; ++i) {
      // pressing 'return' 2 to 10 times should trigger the described behavior
      Thread.sleep(100);

      final OrientGraph graph = factory.getTx();

      // should always be 'local', but eventually changes to 'round-robin'
      System.out.println("StrategyClassName: " + graph.getVertexType("Test").getClusterSelection().getClass().getName());
      System.out.println("ClusterSelectionStrategy for " + graph.getRawGraph().getURL() + ": "
          + graph.getVertexType("Test").getClusterSelection().getName());

      Assert.assertEquals(graph.getVertexType("Test").getClusterSelection().getClass().getName(),
          OLocalClusterWrapperStrategy.class.getName());

      Assert.assertEquals(graph.getVertexType("Test").getClusterSelection().getName(), "round-robin");

      graph.addVertex("class:Test", "firstName", "Roger", "lastName", "Smith");
      graph.getRawGraph().commit();

      graph.shutdown();
    }

    factory.close();
    factory.drop();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-HAClusterStrategyTest";
  }
}
