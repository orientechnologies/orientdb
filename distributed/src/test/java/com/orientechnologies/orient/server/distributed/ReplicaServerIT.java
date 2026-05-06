/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAllocation;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Start 3 servers with only "europe" as master and the others as REPLICA */
public class ReplicaServerIT extends AbstractServerClusterTest {
  static final int SERVERS = 3;

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
    // CHECK REPLICA SERVERS HAVE NO CLUSTER OWNED
    checkReplicasDontOwnAnyClusters();

    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "adminpwd");

      try {
        System.out.println("Creating vertex class Client" + s + " against server " + g + "...");
        OClass t = g.createVertexClass("Client" + s);

        System.out.println("Creating vertex class Knows" + s + " against server " + g + "...");
        g.createEdgeClass("Knows" + s);

        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);
      } finally {
        g.close();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices on server " + s + "...");

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "adminpwd");

      try {
        final OVertex v = g.newVertex("Client" + s);

        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);
      } finally {
        g.close();
      }
    }

    for (int s = 0; s < SERVERS; ++s) {
      System.out.println("Add vertices in TX on server " + s + "...");

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "adminpwd");
      g.begin();

      try {
        final OVertex v = g.newVertex("Client" + s);
        g.save(v);
        g.commit();
        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);

      } finally {
        g.close();
      }
    }

    serverInstance.get(1).shutdownServer();

    checkReplicasDontOwnAnyClusters();

    serverInstance.get(2).shutdownServer();

    checkReplicasDontOwnAnyClusters();
  }

  private void checkReplicasDontOwnAnyClusters() {
    ODatabaseDocumentInternal db =
        serverInstance.get(0).getServerInstance().openDatabase(getDatabaseName());

    for (int s = 1; s < SERVERS; ++s) {
      String nodeName = serverInstance.get(s).getServerInstance().getNodeId().getNode();
      for (OClass cl : db.getMetadata().getSchema().getClasses()) {
        OClassAllocation allocation = cl.getAllocation();
        if (allocation != null) {
          final List<String> clusters = allocation.getAllocationClusters(nodeName);
          Assert.assertTrue(clusters.isEmpty());
        }
      }
    }
  }

  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "replica-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }
}
