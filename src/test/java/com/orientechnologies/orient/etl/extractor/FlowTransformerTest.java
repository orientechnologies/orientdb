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

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;

/**
 * Tests ETL Flow Transformer.
 *
 * @author Luca Garulli
 */
public class FlowTransformerTest extends ETLBaseTest {
  OrientGraph graph;

  @Override
  @BeforeMethod
  public void setUp() {
    graph = new OrientGraph("memory:FlowTransformerTest");
  }

  @Override
  @AfterMethod
  public void tearDown() {
    graph.drop();
  }

  @Test
  public void testSkip() {
    OETLProcessor proc = getProcessor(
        "{source: { content: { value: 'name,surname\nJay,Miner\nJay,Test' } }, extractor : { row: {} },"
            + " transformers: [{csv: {}}, {vertex: {class:'V'}}, {flow:{operation:'skip',if: 'name <> \'Jay\''}},{field:{fieldName:'name', value:'3'}}"
            + "], loader: { orientdb: { dbURL: 'memory:FlowTransformerTest', dbType:'graph' } } }").execute();

    assertEquals(graph.countVertices("V"), 2);

    Iterator<Vertex> it = graph.getVertices().iterator();

    Vertex v1 = it.next();
    Object value1 = v1.getProperty("name");
    assertEquals(value1, "3");

    Vertex v2 = it.next();
    Object value2 = v2.getProperty("name");
    assertEquals(value2, "3");

    graph.command(new OCommandSQL("delete vertex V")).execute();
  }

  @Test
  public void testSkipNever() {
    OETLProcessor proc = getProcessor(
        "{source: { content: { value: 'name,surname\nJay,Miner\nTest,Test' } }, extractor : { row: {} },"
            + " transformers: [{csv: {}}, {vertex: {class:'V'}}, {flow:{operation:'skip',if: 'name = \'Jay\''}},{field:{fieldName:'name', value:'3'}}"
            + "], loader: { orientdb: { dbURL: 'memory:FlowTransformerTest', dbType:'graph'} } }").execute();

    assertEquals(graph.countVertices("V"), 2);

    Iterator<Vertex> it = graph.getVertices().iterator();

    Vertex v1 = it.next();
    Object value1 = v1.getProperty("name");
    assertEquals(value1, "Jay");

    Vertex v2 = it.next();
    Object value2 = v2.getProperty("name");
    assertEquals(value2, "3");

    graph.command(new OCommandSQL("delete vertex V")).execute();
  }
}
