/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.orient.etl.OETLBaseTest;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Tests ETL Flow Transformer.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OFlowTransformerTest extends OETLBaseTest {
  @Test
  public void testSkip() {

    process("{source: { content: { value: 'name,surname\nJay,Miner\nSkipMe,Test' } }, extractor : { csv: {} },"
        + " transformers: [{vertex: {class:'V'}}, "
        + "{flow:{operation:'skip',if: 'name <> \'Jay\''}},"
        + "{field:{fieldName:'name', value:'3'}}"
        + "], loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph' } } }");

    assertEquals(1, graph.countVertices("V"));

    Iterator<Vertex> it = graph.getVertices().iterator();

    Vertex v1 = it.next();
    Object value1 = v1.getProperty("name");
    assertEquals("3", value1);

  }

  @Test
  public void testSkipNever() {
    process("{source: { content: { value: 'name,surname\nJay,Miner\nTest,Test' } }, "
        + "extractor : { csv: {} },"
        + " transformers: ["
        + "{vertex: {class:'V'}}, "
        + "{flow:{operation:'skip',if: 'name = \'Jay\''}},"
        + "{field:{fieldName:'name', value:'3'}}"
        + "],"
        + " loader: { orientdb: {  dbURL: 'memory:OETLBaseTest', dbType:'graph'} } }");

    assertEquals(1, graph.countVertices("V"));

    Iterator<Vertex> it = graph.getVertices().iterator();

    Vertex v1 = it.next();
    Object value1 = v1.getProperty("name");
    assertEquals("3", value1);
    Object value2 = v1.getProperty("surname");
    assertEquals("Test", value2);

  }
}
