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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SQLCreateClassTest {
  @Test
  public void testSimpleCreate() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLCreateClassTest.class.getName());
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
      db.command(new OCommandSQL("create class testSimpleCreate")).execute();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleCreate"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testIfNotExists() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + SQLCreateClassTest.class.getName() + "_ifNotExists");
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      db.command(new OCommandSQL("create class testIfNotExists if not exists")).execute();
      db.getMetadata().getSchema().reload();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      db.command(new OCommandSQL("create class testIfNotExists if not exists")).execute();
      db.getMetadata().getSchema().reload();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfNotExists"));
      try {
        db.command(new OCommandSQL("create class testIfNotExists")).execute();
        Assert.fail();
      } catch (Exception e) {
      }
    } finally {
      db.drop();
    }
  }
}
