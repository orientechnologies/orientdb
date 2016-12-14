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

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests load balancing at client side.
 */
public class ServerClusterRemoteInsertBalancedTest extends AbstractServerClusterTest {
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
    final OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/" + getDatabaseName());

    factory.setConnectionStrategy(OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_CONNECT.toString());

    OrientGraphNoTx graph = factory.getNoTx();
    graph.createVertexType("Client");
    graph.shutdown();

    Map<Integer, Integer> clusterIds = new HashMap<Integer, Integer>();

    for (int i = 0; i < ITERATIONS; ++i) {
      graph = factory.getNoTx();
      try {
        final OrientVertex v = graph.addVertex("class:Client");

        Integer value = clusterIds.get(v.getIdentity().getClusterId());
        if (value == null)
          value = 1;
        else
          value++;

        clusterIds.put(v.getIdentity().getClusterId(), value);

      } finally {
        graph.shutdown();
      }
    }

    Assert.assertTrue(clusterIds.size() > 1);
  }

  private void testRoundRobinOnRequest() {
    final OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/" + getDatabaseName());

    factory.setConnectionStrategy(OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST.toString());

    OrientGraphNoTx graph = factory.getNoTx();

    Map<Integer, Integer> clusterIds = new HashMap<Integer, Integer>();

    try {
      for (int i = 0; i < ITERATIONS; ++i) {
        final OrientVertex v = graph.addVertex("class:Client");

        Integer value = clusterIds.get(v.getIdentity().getClusterId());
        if (value == null)
          value = 1;
        else
          value++;

        clusterIds.put(v.getIdentity().getClusterId(), value);

      }
    } finally {
      graph.shutdown();
    }

    Assert.assertTrue(clusterIds.size() > 1);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-insert-loadbalancing";
  }

}
