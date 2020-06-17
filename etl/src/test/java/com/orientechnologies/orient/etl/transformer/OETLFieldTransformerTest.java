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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLFieldTransformerTest extends OETLBaseTest {

  @Test
  public void testValue() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [{field: {fieldName:'test', value: 33}}], loader: { test: {} } }");
    proc.execute();

    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals(doc.<Integer>field("test"), Integer.valueOf(33));
  }

  @Test
  public void testExpression() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [ {field: {fieldName:'test', expression: 'surname'}}], loader: { test: {} } }");
    proc.execute();
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals("Miner", doc.field("test"));
  }

  @Test
  public void testToLowerCase() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [ {field: {fieldName:'name', expression: '$input.name.toLowerCase()'}}], loader: { test: {} } }");
    proc.execute();
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(2, doc.fields());
    assertEquals("jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
  }

  @Test
  public void testRemove() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, "
            + "extractor : { csv: {} }, "
            + "transformers: [ {field: {fieldName:'surname', operation: 'remove'}}], "
            + "loader: { test: {} } }");

    proc.execute();
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(1, doc.fields());
    assertEquals("Jay", doc.field("name"));
  }

  @Test
  public void testSave() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, "
            + "extractor : { csv: {} }, "
            + "transformers: ["
            + "{field:{log:'DEBUG',fieldName:'@class', value:'Test'}}, "
            + "{field:{log:'DEBUG', fieldName:'test', value: 33, save:true}}"
            + "], "
            + "loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "' } } }");
    proc.execute();
    ODatabaseDocument db = proc.getLoader().getPool().acquire();

    OSchema schema = db.getMetadata().getSchema();
    schema.reload();

    assertThat(schema.getClass("Test")).isNotNull();
    assertEquals(1, db.countClass("Test"));
  }
}
