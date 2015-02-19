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

package com.orientechnologies.orient.graph.blueprints;

import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class GraphValidationTest {

  public static final String URL = "memory:" + GraphValidationTest.class.getSimpleName();

  @BeforeClass
  public static void beforeClass() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    g.drop();
  }

  @Test
  public void testValidationOnVertices() {
    OrientGraphNoTx g1 = new OrientGraphNoTx(URL, "admin", "admin");
    try {
      final OrientVertexType validationTestType = g1.createVertexType("ValidationTest");
      validationTestType.createEdgeProperty(Direction.OUT, "Connection").setMandatory(true);
      validationTestType.createEdgeProperty(Direction.IN, "Connection").setMandatory(true);

      final OrientEdgeType connectionType = g1.createEdgeType("Connection");
      connectionType.createProperty("in", OType.LINK, validationTestType).setMandatory(true);
      connectionType.createProperty("out", OType.LINK, validationTestType).setMandatory(true);
    } finally {
      g1.shutdown();
    }

    OrientGraph g2 = new OrientGraph(URL, "admin", "admin");
    try {
      OrientVertex vertex1 = g2.addTemporaryVertex("ValidationTest");
      OrientVertex vertex2 = g2.addTemporaryVertex("ValidationTest");
      OrientVertex vertex3 = g2.addTemporaryVertex("ValidationTest");
      OrientVertex vertex4 = g2.addTemporaryVertex("ValidationTest");

      vertex1.addEdge("Connection", vertex2);
      vertex2.addEdge("Connection", vertex3);
      vertex3.addEdge("Connection", vertex4);
      vertex4.addEdge("Connection", vertex1);

      g2.commit();

    } finally {
      g2.shutdown();
    }
  }
}
