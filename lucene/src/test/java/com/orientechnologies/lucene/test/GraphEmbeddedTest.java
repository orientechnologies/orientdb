/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Created by enricorisa on 03/09/14.
 */
public class GraphEmbeddedTest extends BaseLuceneTest {

  private OrientGraph graph;

  public GraphEmbeddedTest() {

  }

  @Before
  public void init() {

    graph = new OrientGraph(db, false);
    OrientVertexType type = graph.createVertexType("City");
    type.createProperty("latitude", OType.DOUBLE);
    type.createProperty("longitude", OType.DOUBLE);
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }


  @Test
  public void embeddedTx() {

    //THIS WON'T USE LUCENE INDEXES!!!! see #6997

    graph.getRawGraph().begin();
    graph.addVertex("class:City", new Object[] { "name", "London / a" });
    graph.addVertex("class:City", new Object[] { "name", "Rome" });

    graph.commit();

    Iterable<Vertex> vertexes = graph.getVertices("City", new String[] { "name" }, new Object[] { "London / a" });

    int size = 0;
    for (Vertex v : vertexes) {
      size++;
      Assert.assertNotNull(v);
    }
    Assert.assertEquals(size, 1);

    vertexes = graph.getVertices("City", new String[] { "name" }, new Object[] { "Rome" });

    size = 0;
    for (Vertex v : vertexes) {
      size++;
      Assert.assertNotNull(v);
    }
    Assert.assertEquals(size, 1);
  }

  @Test
  public void testGetVericesFilterClass() {

    graph.createVertexType("One");
    graph.createVertexType("Two");
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "NOTUNIQUE"));

    graph.getRawGraph().begin();
    graph.addVertex("class:One", new Object[] { "name", "Same" });
    graph.addVertex("class:Two", new Object[] { "name", "Same" });

    graph.commit();

    Iterable<Vertex> vertexes = graph.getVertices("One", new String[] { "name" }, new Object[] { "Same" });

    int size = 0;
    for (Vertex v : vertexes) {
      size++;
      Assert.assertNotNull(v);
    }
    Assert.assertEquals(1, size);
  }

}
