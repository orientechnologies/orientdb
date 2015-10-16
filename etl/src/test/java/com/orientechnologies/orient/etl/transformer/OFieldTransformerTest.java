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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class OFieldTransformerTest extends ETLBaseTest {

  @Test
  public void testValue() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [{field: {fieldName:'test', value: 33}}], loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals(33, doc.field("test"));
  }

  @Test
  public void testExpression() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [ {field: {fieldName:'test', expression: 'surname'}}], loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals("Miner", doc.field("test"));
  }

  @Test
  public void testRemove() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [ {field: {fieldName:'surname', operation: 'remove'}}], loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(1, doc.fields());
    assertEquals("Jay", doc.field("name"));
  }

  @Test
  public void testSave() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [{field:{fieldName:'@class', value:'Test'}}, {field:{ fieldName:'test', value: 33, save: true}}], loader: { orientdb: { dbURL: 'memory:ETLBaseTest' } } }");
    assertEquals(1, graph.countVertices("Test"));
  }
}
