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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

/** Check vertex and edge creation are propagated across all the nodes. */
public class ServerClusterGraphIT extends AbstractServerClusterTest {
  static final int SERVERS = 2;
  private OVertex v1;
  private OVertex v2;
  private OVertex v3;

  public String getDatabaseName() {
    return "distributed-queries";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    {
      ODatabaseDocument g =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        g.createVertexClass("Post");
        g.createVertexClass("User");
        g.createEdgeClass("Own");

        g.newVertex("User").save();

        g.command("insert into Post (content, timestamp) values('test', 1)").close();
      } finally {
        g.close();
      }
    }

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g2 =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try (OResultSet result = g2.command("select from Post")) {

        assertTrue(result.hasNext());
        assertNotNull(result.next());

      } finally {
        g2.close();
      }
    }

    {
      ODatabaseDocument g =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      try {
        g.command("create edge Own from (select from User) to (select from Post)").close();
      } finally {
        g.close();
      }
    }

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g2 =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {

        OResultSet result = g2.query("select from Own");
        assertTrue(result.hasNext());
        assertNotNull(result.next());

        result = g2.query("select from Post");
        assertTrue(result.hasNext());

        final OVertex v = result.next().getVertex().get();
        assertNotNull(v);

        final Iterable<OEdge> inEdges = v.getEdges(ODirection.IN);
        assertTrue(inEdges.iterator().hasNext());
        assertNotNull(inEdges.iterator().next());

        result = g2.query("select from User");
        assertTrue(result.hasNext());

        final OVertex v2 = result.next().getVertex().get();
        assertNotNull(v2);

        final Iterable<OEdge> outEdges = v2.getEdges(ODirection.OUT);
        assertTrue(outEdges.iterator().hasNext());
        assertNotNull(outEdges.iterator().next());

      } finally {
        g2.close();
      }
    }
  }
}
