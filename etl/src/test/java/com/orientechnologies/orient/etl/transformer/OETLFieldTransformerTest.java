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

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Tests ETL Field Transformer.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLFieldTransformerTest extends OETLBaseTest {

  @Test
  public void testValue() {
    process(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [{field: {fieldName:'test', value: 33}}], loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals(doc.<Integer>field("test"), Integer.valueOf(33));
  }

  @Test
  public void testExpression() {
    process(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, transformers: [ {field: {fieldName:'test', expression: 'surname'}}], loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(3, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
    assertEquals("Miner", doc.field("test"));
  }

  @Test
  public void testRemove() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, " + "extractor : { csv: {} }, "
        + "transformers: [ {field: {fieldName:'surname', operation: 'remove'}}], " + "loader: { test: {} } }");
    assertEquals(1, getResult().size());

    ODocument doc = getResult().get(0);
    assertEquals(1, doc.fields());
    assertEquals("Jay", doc.field("name"));
  }

  @Test
  public void testSave() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, " + "extractor : { csv: {} }, " + "transformers: ["
        + "{field:{log:'FINE',fieldName:'@class', value:'Test'}}, "
        + "{field:{log:'FINE', fieldName:'test', value: 33, save:true}}" + "], "
        + "loader: { orientdb: { dbURL: 'memory:"+name.getMethodName()+"' } } }");

    OSchema schema = graph.getRawGraph().getMetadata().getSchema();
    schema.reload();

//    schema.getClasses().forEach(c -> System.out.println(c.toString()));

    assertThat(schema.getClass("Test")).isNotNull();
    assertEquals(1, graph.countVertices("Test"));
  }

}
