/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class EdgeSaveShouldNotReinsertVerticesTest {

  private OrientGraphFactory factory;

  @Before
  public void before() {
    factory =
        new OrientGraphFactory(
            "memory:" + EdgeSaveShouldNotReinsertVerticesTest.class.getSimpleName());
    factory.setAutoStartTx(false);
  }

  @After
  public void after() {
    factory.drop();
  }

  @Test
  public void test() {

    final OrientGraph graph = factory.getTx();
    graph
        .createVertexType("Person")
        .createProperty("name", OType.STRING)
        .createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);

    graph.begin();

    final OrientVertex v1 = graph.addVertex("class:Person", "name", "1");
    final OrientVertex v2 = graph.addVertex("class:Person", "name", "2");

    v1.setProperty("name", "2");
    v2.setProperty("name", "1");

    // At this point v1 and v2 are considered new, not updated, by internals of ODirtyManager,
    // during the save of created edge
    // they got reinserted into indexes and storage (?).
    v1.addEdge("edge", v2);

    v1.setProperty("name", "1");
    v2.setProperty("name", "2");

    graph.commit();

    graph.shutdown();
  }
}
