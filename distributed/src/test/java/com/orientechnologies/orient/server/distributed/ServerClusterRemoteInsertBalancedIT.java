/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Tests load balancing at client side. */
public class ServerClusterRemoteInsertBalancedIT extends AbstractServerClusterTest {
  private static final int ITERATIONS = 10;

  @Test
  public void test() throws Exception {
    init(2);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {

    testRoundRobinOnConnect();
    testRoundRobinOnRequest();
  }

  private void testRoundRobinOnConnect() {
    ODatabasePool pool =
        new ODatabasePool(
            "remote:localhost/" + getDatabaseName(),
            "admin",
            "admin",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());

    ODatabaseDocument graph = pool.acquire();
    graph.createVertexClass("Client");
    graph.close();

    Map<Integer, Integer> clusterIds = new HashMap<Integer, Integer>();

    for (int i = 0; i < ITERATIONS; ++i) {
      graph = pool.acquire();
      try {
        final OVertex v = graph.newVertex("Client").save();

        Integer value = clusterIds.get(v.getIdentity().getClusterId());
        if (value == null) value = 1;
        else value++;

        clusterIds.put(v.getIdentity().getClusterId(), value);

      } finally {
        graph.close();
      }
    }
    pool.close();

    Assert.assertTrue(clusterIds.size() > 1);
  }

  private void testRoundRobinOnRequest() {
    ODatabasePool factory =
        new ODatabasePool(
            "remote:localhost/" + getDatabaseName(),
            "admin",
            "admin",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());
    ODatabaseDocument graph = factory.acquire();

    Map<Integer, Integer> clusterIds = new HashMap<Integer, Integer>();

    try {
      for (int i = 0; i < ITERATIONS; ++i) {
        final OVertex v = graph.newVertex("Client").save();

        Integer value = clusterIds.get(v.getIdentity().getClusterId());
        if (value == null) value = 1;
        else value++;

        clusterIds.put(v.getIdentity().getClusterId(), value);
      }
    } finally {
      graph.close();
    }

    Assert.assertTrue(clusterIds.size() > 1);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-insert-loadbalancing";
  }
}
