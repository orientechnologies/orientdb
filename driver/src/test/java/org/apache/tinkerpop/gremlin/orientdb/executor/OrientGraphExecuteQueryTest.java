/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.orientdb.executor;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphBaseTest;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 14/11/16. */
public class OrientGraphExecuteQueryTest extends OrientGraphBaseTest {

  @Test
  public void testExecuteGremlinSimpleQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V()", null);

    Assert.assertEquals(2, gremlin.stream().count());
  }

  @Test
  public void testExecuteGremlinCountQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V().count()", null);

    Iterator<OGremlinResult> iterator = gremlin.iterator();
    Assert.assertEquals(true, iterator.hasNext());
    OGremlinResult result = iterator.next();
    Long count = result.getProperty("value");
    Assert.assertEquals(new Long(2), count);
  }

  @Test
  public void testExecuteGremlinVertexQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin =
        noTx.execute("gremlin", "g.V().hasLabel('Person').has('name','Luke')", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();
    OrientVertex vertex = result.getVertex().get();
    Assert.assertEquals("Luke", vertex.value("name"));
  }

  @Test
  public void testExecuteGremlinEdgeQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    Vertex v1 = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex v2 = noTx.addVertex(T.label, "Person", "name", "Luke");

    v1.addEdge("HasFriend", v2, "since", new Date());

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.E().hasLabel('HasFriend')", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();
    OrientEdge vertex = result.getEdge().get();
    Assert.assertNotNull(vertex.value("since"));
  }

  @Test
  public void testExecuteGremlinPathQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    Vertex v1 = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex v2 =
        noTx.addVertex(
            T.label,
            "Person",
            "name",
            "Luke",
            "values",
            new ArrayList<String>() {
              {
                add("first");
                add("second");
              }
            });

    v1.addEdge("HasFriend", v2, "since", new Date());

    OGremlinResultSet gremlin =
        noTx.execute("gremlin", "g.V().has('name','John').out().values('values').path()", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();

    List results = result.getProperty("value");

    Assert.assertEquals(3, results.size());

    Assert.assertTrue(results.get(0) instanceof OGremlinResult);

    OGremlinResult r = (OGremlinResult) results.get(0);
    Assert.assertTrue(r.getVertex().isPresent());

    Assert.assertTrue(results.get(1) instanceof OGremlinResult);

    r = (OGremlinResult) results.get(1);
    Assert.assertTrue(r.getVertex().isPresent());

    Assert.assertTrue(results.get(2) instanceof Collection);

    List coll = (List) results.get(2);

    Assert.assertEquals(2, coll.size());
  }
}
