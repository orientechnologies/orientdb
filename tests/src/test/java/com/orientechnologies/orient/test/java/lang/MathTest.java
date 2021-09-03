/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.java.lang;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.Assert;

public class MathTest {
  public static final void main(String[] args) {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:test").create();
    final OSchema schema = db.getMetadata().getSchema();
    final OClass clazz = schema.createClass("test");
    clazz.createProperty("numericAtt", OType.DOUBLE);

    db.command(new OCommandSQL("INSERT INTO test(numericAtt) VALUES (28.23)")).execute();

    final List<ODocument> docs = db.query(new OSQLSynchQuery("SELECT FROM test"));
    Double value = (Double) docs.get(0).field("numericAtt");

    System.out.println(value);

    Assert.assertEquals(new Double(28.23), new Float(28.23).doubleValue());
    Assert.assertEquals(new Float(28.23), new Double(28.23).floatValue());
  }
}
