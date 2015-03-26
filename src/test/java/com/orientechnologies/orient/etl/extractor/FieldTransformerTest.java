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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;
import org.testng.annotations.Test;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli
 */
public class FieldTransformerTest extends ETLBaseTest {

  @Test
  public void testValue() {
    OETLProcessor proc = getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'test', value: 33}}], loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(doc.fields(), 3);
    assertEquals(doc.field("name"), "Jay");
    assertEquals(doc.field("surname"), "Miner");
    assertEquals(doc.field("test"), 33);
  }

  @Test
  public void testExpression() {
    OETLProcessor proc = getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'test', expression: 'surname'}}], loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(doc.fields(), 3);
    assertEquals(doc.field("name"), "Jay");
    assertEquals(doc.field("surname"), "Miner");
    assertEquals(doc.field("test"), "Miner");
  }

  @Test
  public void testRemove() {
    OETLProcessor proc = getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field: {fieldName:'surname', operation: 'remove'}}], loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);

    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(doc.fields(), 1);
    assertEquals(doc.field("name"), "Jay");
  }

  @Test
  public void testSave() {
    OETLProcessor proc = getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }, {field:{fieldName:'@class', value:'Test'}}, {field:{ fieldName:'test', value: 33, save: true}}], loader: { orientdb: { dbURL: 'memory:FieldTransformerTest' } } }").execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:FieldTransformerTest").open("admin", "admin");
    assertEquals(db.countClass("Test"), 1);
  }
}
