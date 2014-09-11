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

package com.orientechnologies.test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.annotations.Test;

/**
 * Created by enricorisa on 03/09/14.
 */
public class TestEmbedded {

  private static String url;
  static {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    url = "plocal:" + buildDirectory + "/embeddedTest";
  }

  @Test
  public void embedded() {
    OrientGraph graph = new OrientGraph(url, true);

    OrientVertexType type = graph.createVertexType("City");
    type.createProperty("latitude", OType.DOUBLE);
    type.createProperty("longitude", OType.DOUBLE);
    type.createProperty("name", OType.STRING);
    type.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });

    graph.addVertex("class:City", new Object[] { "name", "London" });
    graph.addVertex("class:City", new Object[] { "name", "Rome" });

    graph.commit();

    Iterable<Vertex> vertexes = graph.getVertices("City", new String[] { "name" }, new Object[] { "London" });

    for (Vertex v : vertexes) {
      System.out.println(v.getId());
    }
    graph.drop();
  }
}
