/*
 *
 *  * Copyright 2014 Orient Technologies.
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

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    graph.getRawGraph().begin();
    graph.addVertex("class:City", new Object[] { "name", "London" });
    graph.addVertex("class:City", new Object[] { "name", "Rome" });

    graph.commit();

    Iterable<Vertex> vertexes = graph.getVertices("City", new String[] { "name" }, new Object[] { "London" });

    int size = 0;
    for (Vertex v : vertexes) {
      size++;
      Assert.assertNotNull(v);
    }
    Assert.assertEquals(size, 1);
  }

}
