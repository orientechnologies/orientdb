/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DocumentTest {
  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + DocumentTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  public void testFromMapNotSaved() {
    final ODocument doc = new ODocument();
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 2);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
  }

  public void testFromMapWithClass() {
    final ODocument doc = new ODocument("OUser");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 3);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
    Assert.assertEquals(map.get("@class"), "OUser");
  }

  public void testFromMapWithClassAndRid() {
    final ODocument doc = new ODocument("V");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    doc.save();
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 4);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
    Assert.assertEquals(map.get("@class"), "V");
    Assert.assertTrue(map.containsKey("@rid"));
  }

  @Test
  public void testConversionOnTypeSet() {
    ODocument doc = new ODocument();

    doc.field("some", 3);
    doc.setFieldType("some", OType.STRING);
    Assert.assertEquals(doc.fieldType("some"), OType.STRING);
    Assert.assertEquals(doc.field("some"), "3");
  }

  @Test
  public void testEval() {
    ODocument doc = new ODocument();

    doc.field("amount", 300);

    Number amountPlusVat = (Number) doc.eval("amount * 120 / 100");

    Assert.assertEquals(amountPlusVat.longValue(), 360l);
  }

  @Test
  public void testEvalInContext() {
    ODocument doc = new ODocument();

    doc.field("amount", 300);

    OCommandContext context = new OBasicCommandContext().setVariable("vat", 20);
    Number amountPlusVat = (Number) doc.eval("amount * (100 + $vat) / 100", context);

    Assert.assertEquals(amountPlusVat.longValue(), 360l);
  }
}
