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

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.distributed.impl.OLocalClusterWrapperStrategy;
import org.junit.Assert;
import org.junit.Test;

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
    String dbUrl = getDatabaseURL(serverInstance.get(0));
    OrientDB orientDB = new OrientDB(dbUrl.substring(0, dbUrl.length() - (getDatabaseName().length() + 1)), "root", "root",
        OrientDBConfig.defaultConfig());
    final ODatabasePool pool = new ODatabasePool(orientDB, getDatabaseName(), "admin", "admin");

    final ODatabaseDocument g = pool.acquire();
    g.createVertexClass("Test");
    g.close();

    for (int i = 0; i < 10; ++i) {
      // pressing 'return' 2 to 10 times should trigger the described behavior
      Thread.sleep(100);

      final ODatabaseDocument graph = pool.acquire();
      graph.begin();

      // should always be 'local', but eventually changes to 'round-robin'
      System.out.println("StrategyClassName: " + graph.getClass("Test").getClusterSelection().getClass().getName());
      System.out.println(
          "ClusterSelectionStrategy for " + graph.getURL() + ": " + graph.getClass("Test").getClusterSelection().getName());

      Assert.assertEquals(graph.getClass("Test").getClusterSelection().getClass().getName(),
          OLocalClusterWrapperStrategy.class.getName());

      Assert.assertEquals(graph.getClass("Test").getClusterSelection().getName(), "round-robin");

      OVertex v = graph.newVertex("Test");
      v.setProperty("firstName", "Roger");
      v.setProperty("lastName", "Smith");
      v.save();
      graph.commit();

      graph.close();
    }

    pool.close();
    orientDB.drop(getDatabaseName());
    orientDB.close();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-HAClusterStrategyTest";
  }
}
