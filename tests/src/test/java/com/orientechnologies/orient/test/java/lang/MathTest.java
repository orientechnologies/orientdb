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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;

public class MathTest {
  public static final void main(String[] args) {
    final ODatabaseDocument db = new ODatabaseDocumentTx("memory:test").create();
    final OSchema schema = db.getMetadata().getSchema();
    final OClass clazz = schema.createClass("test");
    clazz.createProperty("numericAtt", OType.DOUBLE);

    db.command("INSERT INTO test(numericAtt) VALUES (28.23)").close();

    final List<OResult> docs = db.query("SELECT FROM test").stream().collect(Collectors.toList());
    Double value = (Double) docs.get(0).getProperty("numericAtt");

    System.out.println(value);

    Assert.assertEquals(new Double(28.23), new Float(28.23).doubleValue());
    Assert.assertEquals(new Float(28.23), new Double(28.23).floatValue());
  }
}
