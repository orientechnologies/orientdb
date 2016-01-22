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

import org.junit.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Check vertex and edge creation are propagated across all the nodes.
 */
public class ServerClusterGraphTest extends AbstractServerClusterTest {
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
    {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();

      try {
        g.createVertexType("Post");
        g.createVertexType("User");
        g.createEdgeType("Own");

        g.addVertex("class:User");

        g.command(new OCommandSQL("insert into Post (content, timestamp) values('test', 1)")).execute();
      } finally {
        g.shutdown();
      }
    }

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory2 = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g2 = factory2.getNoTx();

      try {

        Iterable<OrientVertex> result = g2.command(new OCommandSQL("select from Post")).execute();
        Assert.assertTrue(result.iterator().hasNext());
        Assert.assertNotNull(result.iterator().next());

      } finally {
        g2.shutdown();
      }
    }

    {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
      OrientGraphNoTx g = factory.getNoTx();
      try {
        g.command(new OCommandSQL("create edge Own from (select from User) to (select from Post)")).execute();
      } finally {
        g.shutdown();
      }
    }

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory2 = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName());
      OrientGraphNoTx g2 = factory2.getNoTx();

      try {

        Iterable<OrientVertex> result = g2.command(new OCommandSQL("select from Own")).execute();
        Assert.assertTrue(result.iterator().hasNext());
        Assert.assertNotNull(result.iterator().next());

        result = g2.command(new OCommandSQL("select from Post")).execute();
        Assert.assertTrue(result.iterator().hasNext());

        final OrientVertex v = result.iterator().next();
        Assert.assertNotNull(v);

        final Iterable<Edge> inEdges = v.getEdges(Direction.IN);
        Assert.assertTrue(inEdges.iterator().hasNext());
        Assert.assertNotNull(inEdges.iterator().next());

        result = g2.command(new OCommandSQL("select from User")).execute();
        Assert.assertTrue(result.iterator().hasNext());

        final OrientVertex v2 = result.iterator().next();
        Assert.assertNotNull(v2);

        final Iterable<Edge> outEdges = v2.getEdges(Direction.OUT);
        Assert.assertTrue(outEdges.iterator().hasNext());
        Assert.assertNotNull(outEdges.iterator().next());

      } finally {
        g2.shutdown();
      }
    }

  }
}
