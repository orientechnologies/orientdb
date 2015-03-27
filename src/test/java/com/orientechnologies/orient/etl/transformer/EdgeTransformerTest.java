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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.Test;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class EdgeTransformerTest extends ETLBaseTest {

  @Override
  public void setUp() {
    super.setUp();
    graph.createVertexType("V1");
    graph.createVertexType("V2");
    graph.createEdgeType("Friend");

    graph.addVertex("class:V2").setProperty("name", "Luca");
    graph.commit();
  }

  @Test
  public void testNotLightweightEdge() {
    process("{source: { content: { value: 'name,surname,friend\nJay,Miner,Luca' } }, extractor : { row: {} },"
        + " transformers: [{csv: {}}, {vertex: {class:'V1'}}, {edge:{class:'Friend',joinFieldName:'friend',lookup:'V2.name'}},"
        + "], loader: { orientdb: { dbURL: 'memory:ETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertEquals(1, graph.countVertices("V1"));
    assertEquals(1, graph.countVertices("V2"));
    assertEquals(1, graph.countEdges("Friend"));
  }
}
