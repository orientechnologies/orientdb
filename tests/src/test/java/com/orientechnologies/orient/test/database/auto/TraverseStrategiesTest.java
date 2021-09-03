/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** @author marko */
@Test
public class TraverseStrategiesTest extends DocumentDBBaseTest {

  private int totalElements = 0;

  public TraverseStrategiesTest() {
    super("memory:traverseStrategyTest");
  }

  @BeforeClass
  public void init() {
    OrientGraph graph = new OrientGraph(database);
    graph.setUseLightweightEdges(false);

    graph.createVertexType("tc");
    graph.createEdgeType("te");

    ODocument first = graph.addVertex("class:tc", "name", 1.0).getRecord();
    ++totalElements;
    ODocument firstFirstChild = graph.addVertex("class:tc", "name", 1.1).getRecord();
    ++totalElements;
    ODocument firstSecondChild = graph.addVertex("class:tc", "name", 1.2).getRecord();
    ++totalElements;
    ODocument second = graph.addVertex("class:tc", "name", 2.0).getRecord();
    ++totalElements;
    ODocument secondFirstCHild = graph.addVertex("class:tc", "name", 2.1).getRecord();
    ++totalElements;
    ODocument secondSecondChild = graph.addVertex("class:tc", "name", 2.2).getRecord();
    ++totalElements;

    graph.addEdge(this, graph.getVertex(first), graph.getVertex(firstFirstChild), "te");
    graph.addEdge(this, graph.getVertex(first), graph.getVertex(firstSecondChild), "te");
    graph.addEdge(this, graph.getVertex(second), graph.getVertex(secondFirstCHild), "te");
    graph.addEdge(this, graph.getVertex(second), graph.getVertex(secondSecondChild), "te");

    graph.commit();
  }

  @Test
  public void getAllRevresedBreadthFirst() {
    OResultSet result1 =
        database.query(
            "traverse in(\"te\") from (select rids from (select @rid as rids, out(\"te\") as outEdge from tc unwind outEdge) where outEdge is null) strategy BREADTH_FIRST");

    for (int i = 0; i < totalElements; i++) {
      Assert.assertTrue(result1.hasNext());
      OResult result = result1.next();
      OVertex v = result.getVertex().orElse(null);
      Assert.assertNotNull(v);
    }

    Assert.assertFalse(result1.hasNext());

    result1.close();
  }
}
