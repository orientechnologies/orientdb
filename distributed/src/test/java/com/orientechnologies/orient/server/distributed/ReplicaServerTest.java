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

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import junit.framework.Assert;
import org.junit.Test;

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
      ODatabasePool factory = OrientDB.fromUrl("embedded:target/server" + s + "/databases/", OrientDBConfig.defaultConfig())
          .openPool(getDatabaseName(), "admin", "admin");
      ODatabaseDocument g = factory.acquire();

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

      ODatabasePool factory = OrientDB.fromUrl("embedded:target/server" + s + "/databases/", OrientDBConfig.defaultConfig())
          .openPool(getDatabaseName(), "admin", "admin");

      ODatabaseDocument g = factory.acquire();

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


      ODatabasePool factory = OrientDB.fromUrl("embedded:target/server" + s + "/databases/", OrientDBConfig.defaultConfig())
          .openPool(getDatabaseName(), "admin", "admin");

      ODatabaseDocument g = factory.acquire();
      g.begin();

      try {
        final OVertex v = g.newVertex("Client" + s).save();

        g.commit();
        Assert.assertTrue(s == 0);

      } catch (Exception e) {
        Assert.assertTrue(s > 0);

      } finally {
        g.close();
      }
    }
  }

  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "replica-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }
}
