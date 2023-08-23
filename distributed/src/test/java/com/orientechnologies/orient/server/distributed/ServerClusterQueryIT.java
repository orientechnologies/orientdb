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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

/** Start 2 servers and execute query across the cluster */
public class ServerClusterQueryIT extends AbstractServerClusterTest {
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
    createDatabase();

    // TEST CASES
    checkNestedQueryContext();
    checkSum();
    checkShardedOrderBy();
    checkShardedGroupBy();
  }

  private void createDatabase() {
    ODatabaseDocument g =
        serverInstance.get(0).getServerInstance().openDatabase(getDatabaseName(), "admin", "admin");

    try {
      g.createVertexClass("V1");
      g.createEdgeClass("E1");
      v1 = g.newVertex("V1");
      v1.setProperty("amount", 10);
      v1.setProperty("kind", "a");
      v1.save();

      v2 = g.newVertex("V1");
      v2.setProperty("amount", 15);
      v2.setProperty("kind", "b");
      v2.save();

      v3 = g.newVertex("V1");
      v3.setProperty("amount", 21);
      v3.setProperty("kind", "b");
      v3.save();

      v1.addEdge(v2, "E1").save();
    } finally {
      g.close();
    }
  }

  private void checkNestedQueryContext() {
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        OResultSet it =
            g.query("select *, $depth as d from (traverse in('E1') from ?)", v2.getIdentity());

        assertTrue(it.hasNext());

        final OResult r1 = it.next();
        assertTrue(it.hasNext());
        final OResult r2 = it.next();
        assertFalse(it.hasNext());

        assertEquals(r1.<Object>getProperty("d"), 0);
        assertEquals(r2.<Object>getProperty("d"), 1);

      } finally {
        g.close();
      }
    }
  }

  private void checkSum() {
    for (int s = 0; s < SERVERS; ++s) {

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        OResultSet it = g.command("select sum(amount) as total from v", v2.getIdentity());

        assertTrue(it.hasNext());

        final OResult r1 = it.next();
        assertEquals(r1.<Object>getProperty("total"), 46);

      } finally {
        g.close();
      }
    }
  }

  private void checkShardedOrderBy() {
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        OResultSet it = g.command("select amount from v order by amount asc");

        assertTrue(it.hasNext());

        OResult r1 = it.next();
        assertTrue(it.hasNext());
        OResult r2 = it.next();
        assertTrue(it.hasNext());
        OResult r3 = it.next();
        assertFalse(it.hasNext());

        assertEquals(10, r1.<Object>getProperty("amount"));
        assertEquals(15, r2.<Object>getProperty("amount"));
        assertEquals(21, r3.<Object>getProperty("amount"));

        it = g.command("select amount from v order by amount desc");

        assertTrue(it.hasNext());

        r1 = it.next();
        assertTrue(it.hasNext());
        r2 = it.next();
        assertTrue(it.hasNext());
        r3 = it.next();
        assertFalse(it.hasNext());

        assertEquals(21, r1.<Object>getProperty("amount"));
        assertEquals(15, r2.<Object>getProperty("amount"));
        assertEquals(10, r3.<Object>getProperty("amount"));

      } finally {
        g.close();
      }
    }
  }

  private void checkShardedGroupBy() {
    for (int s = 0; s < SERVERS; ++s) {
      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");

      try {
        OResultSet it =
            g.query("select from ( select amount, kind from v group by kind ) order by kind");

        assertTrue(it.hasNext());

        OResult r1 = it.next();
        assertTrue(it.hasNext());
        OResult r2 = it.next();
        // assertFalse(it.hasNext());

        assertEquals(r1.getProperty("kind"), "a");
        assertEquals(r2.getProperty("kind"), "b");

      } finally {
        g.close();
      }
    }
  }
}
