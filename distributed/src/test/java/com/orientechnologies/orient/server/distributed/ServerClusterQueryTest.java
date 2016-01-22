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

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Start 2 servers and execute query across the cluster
 */
public class ServerClusterQueryTest extends AbstractServerClusterTest {
  final static int     SERVERS = 2;
  private OrientVertex v1;
  private OrientVertex v2;
  private OrientVertex v3;

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
    OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphNoTx g = factory.getNoTx();

    try {
      g.createVertexType("V1");
      g.createEdgeType("E1");
      v1 = g.addVertex("class:V1");
      v1.setProperty("amount", 10);
      v1.setProperty("kind", "a");

      v2 = g.addVertex("class:V2");
      v2.setProperty("amount", 15);
      v2.setProperty("kind", "b");

      v3 = g.addVertex("class:V2");
      v3.setProperty("amount", 21);
      v3.setProperty("kind", "b");

      v1.addEdge("E1", v2);
    } finally {
      g.shutdown();
    }
  }

  private void checkNestedQueryContext() {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        final Iterable<OrientVertex> result = g.command(new OCommandSQL("select *, $depth as d from (traverse in('E1') from ?)"))
            .execute(v2.getIdentity());

        final Iterator<OrientVertex> it = result.iterator();
        Assert.assertTrue(it.hasNext());

        final OrientVertex r1 = it.next();
        Assert.assertTrue(it.hasNext());
        final OrientVertex r2 = it.next();
        Assert.assertFalse(it.hasNext());

        Assert.assertEquals(r1.getProperty("d"), 0);
        Assert.assertEquals(r2.getProperty("d"), 1);

      } finally {
        g.shutdown();
      }
    }
  }

  private void checkSum() {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        final Iterable<OrientVertex> result = g.command(new OCommandSQL("select sum(amount) as total from v")).execute(
            v2.getIdentity());

        final Iterator<OrientVertex> it = result.iterator();
        Assert.assertTrue(it.hasNext());

        final OrientVertex r1 = it.next();
        Assert.assertEquals(r1.getProperty("total"), 46);

      } finally {
        g.shutdown();
      }
    }
  }

  private void checkShardedOrderBy() {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        Iterable<OrientVertex> result = g.command(new OCommandSQL("select amount from v order by amount asc")).execute(
            v2.getIdentity());

        Iterator<OrientVertex> it = result.iterator();
        Assert.assertTrue(it.hasNext());

        OrientVertex r1 = it.next();
        Assert.assertTrue(it.hasNext());
        OrientVertex r2 = it.next();
        Assert.assertTrue(it.hasNext());
        OrientVertex r3 = it.next();
        Assert.assertFalse(it.hasNext());

        Assert.assertEquals(10, r1.getProperty("amount"));
        Assert.assertEquals(15, r2.getProperty("amount"));
        Assert.assertEquals(21, r3.getProperty("amount"));

        result = g.command(new OCommandSQL("select amount from v order by amount desc")).execute(v2.getIdentity());

        it = result.iterator();
        Assert.assertTrue(it.hasNext());

        r1 = it.next();
        Assert.assertTrue(it.hasNext());
        r2 = it.next();
        Assert.assertTrue(it.hasNext());
        r3 = it.next();
        Assert.assertFalse(it.hasNext());

        Assert.assertEquals(21, r1.getProperty("amount"));
        Assert.assertEquals(15, r2.getProperty("amount"));
        Assert.assertEquals(10, r3.getProperty("amount"));

      } finally {
        g.shutdown();
      }
    }
  }

  private void checkShardedGroupBy() {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        Iterable<OrientVertex> result = g.command(
            new OCommandSQL("select from ( select amount, kind from v group by kind ) order by kind")).execute();

        Iterator<OrientVertex> it = result.iterator();
        Assert.assertTrue(it.hasNext());

        OrientVertex r1 = it.next();
        Assert.assertTrue(it.hasNext());
        OrientVertex r2 = it.next();
        Assert.assertFalse(it.hasNext());

        Assert.assertEquals(r1.getProperty("kind"), "a");
        Assert.assertEquals(r2.getProperty("kind"), "b");

      } finally {
        g.shutdown();
      }
    }
  }
}
