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

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class EdgeTransformerTest extends ETLBaseTest {
  OrientGraph graph;

  public void testNotLightweightEdge() {
    OETLProcessor proc = getProcessor(
        "{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca' } }, extractor : { row: {} },"
            + " transformers: [{csv: {}}, {vertex: {class:'V1'}}, {edge:{class:'Friend',joinFieldName:'friend',lookup:'V2.name'}},"
            + "], loader: { orientdb: { dbURL: 'memory:EdgeTransformerTest', dbType:'graph', useLightweightEdges:false } } }")
        .execute();

    assertEquals(graph.countVertices("V1"), 1);
    assertEquals(graph.countVertices("V2"), 1);
    assertEquals(graph.countEdges("Friend"), 1);
  }

  public void setUp() {
    graph = new OrientGraph("memory:EdgeTransformerTest");
    graph.setUseLightweightEdges(false);

    graph.createVertexType("V1");
    graph.createVertexType("V2");
    graph.createEdgeType("Friend");

    graph.addVertex("class:V2").setProperty("name", "Luca");
    graph.commit();
  }

  public void tearDown() {
    graph.drop();
  }
}
