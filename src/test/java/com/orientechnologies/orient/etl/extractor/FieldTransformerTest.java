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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.TestLoader;
import org.junit.Test;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class FieldTransformerTest extends ETLBaseTest {

  @Test
  public void testValue() {
    getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'test', value: 33}}], loader: { test: {} } }").execute();
    assertEquals(1, ((TestLoader) proc.getLoader()).getResult().size());

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals(33, doc.field("test"));
  }

  @Test
  public void testExpression() {
    getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'test', expression: 'surname'}}], loader: { test: {} } }").execute();
    assertEquals(1, ((TestLoader) proc.getLoader()).getResult().size());

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals("Miner", doc.field("test"));
  }

  @Test
  public void testRemove() {
    getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'surname', operation: 'remove'}}], loader: { test: {} } }").execute();
    assertEquals(1, ((TestLoader) proc.getLoader()).getResult().size());

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(1, doc.fields());
    assertEquals("Jay", doc.field("name"));
  }

  @Test
  public void testSave() {
    getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field:{fieldName:'@class', value:'Test'}}, {field:{ fieldName:'test', value: 33, save: true}}], loader: { orientdb: { dbURL: 'memory:ETLBaseTest' } } }").execute();
    assertEquals(1, graph.countVertices("Test"));
  }
}
