/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.test;

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class OrientFilterByClassTest {

  @Test
  public void test() {
    OrientGraphFactory gf = new OrientGraphFactory("memory:temp");
    OrientGraphNoTx graph = gf.getNoTx();
    graph.createVertexType("A");
    graph.createVertexType("B");
    graph.createVertexType("C");

    OrientVertex a1 = graph.addVertex("class:A");
    assertEquals(1, graph.countVertices("A"));

    OrientVertex b1 = graph.addVertex("class:B");
    assertEquals(1, graph.countVertices("B"));
    OrientVertex c1 = graph.addVertex("class:C");
    OrientVertex c2 = graph.addVertex("class:C");
    assertEquals(2, graph.countVertices("C"));

    graph.addEdge(null, b1, a1, "link");
    graph.addEdge(null, c1, a1, "link");
    graph.addEdge(null, c2, a1, "link");

    Iterable<OrientVertex> resultsQ1 = graph.command(new OSQLSynchQuery<OrientVertex>("SELECT set(in('link')) as inLink FROM V"))
        .execute();
    assertTrue(resultsQ1.iterator().hasNext());

    Iterable<OrientVertex> resultsQ2 = graph.command(
        new OSQLSynchQuery<OrientVertex>("SELECT set(in('link')[@class='B']) as inLink FROM V")).execute();
    assertTrue(resultsQ2.iterator().hasNext());

    Iterable<Object> s1 = resultsQ1.iterator().next().getProperty("inLink");
    assertTrue(s1.iterator().hasNext());

    Iterable<Object> s2 = resultsQ2.iterator().next().getProperty("inLink");
    assertTrue(s2.iterator().hasNext());

    Object o1 = null;
    for (Object o : s1) {
      o1 = o;
      break;
    }

    Object o2 = null;
    for (Object o : s2) {
      o2 = o;
      break;
    }

    graph.shutdown();

    assertEquals(o1.getClass().toString(), o2.getClass().toString());

  }

}
