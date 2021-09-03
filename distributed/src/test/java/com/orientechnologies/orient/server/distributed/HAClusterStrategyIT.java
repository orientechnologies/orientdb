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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Assert;
import org.junit.Test;

public class HAClusterStrategyIT extends AbstractHARemoveNode {
  private static final int SERVERS = 2;

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
    final ODatabaseDocument g =
        serverInstance.get(0).getServerInstance().openDatabase(getDatabaseName(), "admin", "admin");

    g.createVertexClass("Test");
    g.close();

    for (int i = 0; i < 10; ++i) {
      // pressing 'return' 2 to 10 times should trigger the described behavior
      Thread.sleep(100);

      final ODatabaseDocument graph =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      graph.begin();

      // should always be 'local', but eventually changes to 'round-robin'
      System.out.println(
          "StrategyClassName: "
              + graph.getClass("Test").getClusterSelection().getClass().getName());
      System.out.println(
          "ClusterSelectionStrategy for "
              + graph.getURL()
              + ": "
              + graph.getClass("Test").getClusterSelection().getName());

      Assert.assertEquals(graph.getClass("Test").getClusterSelection().getName(), "round-robin");

      OVertex v = graph.newVertex("Test");
      v.setProperty("firstName", "Roger");
      v.setProperty("lastName", "Smith");
      v.save();

      graph.commit();

      graph.close();
    }

    serverInstance.get(0).getServerInstance().dropDatabase(getDatabaseName());
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-HAClusterStrategyIT";
  }
}
