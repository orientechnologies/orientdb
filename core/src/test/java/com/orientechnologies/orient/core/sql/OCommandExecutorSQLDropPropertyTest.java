/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class OCommandExecutorSQLDropPropertyTest {
  private static String DB_STORAGE = "memory";
  private static String DB_NAME    = "OCommandExecutorSQLDropPropertyTest";

  private int ORDER_SKIP_LIMIT_ITEMS = 100 * 1000;

  ODatabaseDocumentTx db;

  @BeforeClass public void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();

  }

  @Test public void test() {
    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass foo = schema.createClass("Foo");

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command(new OCommandSQL("DROP PROPERTY Foo.name")).execute();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command(new OCommandSQL("DROP PROPERTY `Foo`.name")).execute();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command(new OCommandSQL("DROP PROPERTY Foo.`name`")).execute();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command(new OCommandSQL("DROP PROPERTY `Foo`.`name`")).execute();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));
  }

}