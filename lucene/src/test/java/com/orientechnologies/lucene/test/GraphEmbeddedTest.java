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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Created by enricorisa on 03/09/14.
 */
@Test(groups = "embedded")
public class GraphEmbeddedTest extends BaseLuceneTest {

  private OrientGraph graph;

  public GraphEmbeddedTest() {

  }

  public GraphEmbeddedTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void init() {
    initDB();
    graph = new OrientGraph((ODatabaseDocumentTx) databaseDocumentTx, false);
    OrientVertexType type = graph.createVertexType("City");
    type.createProperty("latitude", OType.DOUBLE);
    type.createProperty("longitude", OType.DOUBLE);
    type.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  @Test
  public void embedded() {

    graph.getRawGraph().begin();
    graph.addVertex("class:City", new Object[] { "name", "London" });
    graph.addVertex("class:City", new Object[] { "name", "Rome" });

    graph.commit();

    Iterable<Vertex> vertexes = graph.getVertices("City", new String[] { "name" }, new Object[] { "London" });

    int size = 0;
    for (Vertex v : vertexes) {
      size++;
    }
    Assert.assertEquals(size, 1);
  }

  @Override
  protected String getDatabaseName() {
    return "graphEmbedded";
  }
}
