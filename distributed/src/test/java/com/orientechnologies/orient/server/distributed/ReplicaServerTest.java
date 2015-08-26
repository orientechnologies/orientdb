/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.junit.Test;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Start 3 servers with only "europe" as master and the others as REPLICA
 */
public class ReplicaServerTest extends AbstractServerClusterTest {
  final static int SERVERS = 3;

  public String getDatabaseName() {
    return "distributed-replicatest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        System.out.println("Creating vertex class Client" + s + " against server " + g + "...");
        OrientVertexType t = g.createVertexType("Client" + s);

        System.out.println("Creating vertex class Knows" + s + " against server " + g + "...");
        g.createEdgeType("Knows" + s);

        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);
      } finally {
        g.shutdown();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices on server " + s + "...");

      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        final OrientVertex v = g.addVertex("class:" + "Client" + s);

        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);
      } finally {
        g.shutdown();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices in TX on server " + s + "...");

      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraph g = factory.getTx();

      try {
        final OrientVertex v = g.addVertex("class:" + "Client" + s);
        g.commit();
        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);

      } finally {
        g.shutdown();
      }
    }
  }

  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "replica-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }
}
