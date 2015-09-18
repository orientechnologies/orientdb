/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

@Test
public class OCommandExecutorSQLSelectTestIndex {

  @Test
  public void testIndexSqlEmbeddedList() {

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("memory:embeddedList");
    databaseDocumentTx.create();
    try {

      databaseDocumentTx.command(new OCommandSQL("create class Foo")).execute();
      databaseDocumentTx.command(new OCommandSQL("create property Foo.bar EMBEDDEDLIST STRING")).execute();
      databaseDocumentTx.command(new OCommandSQL("create index Foo.bar on Foo (bar) NOTUNIQUE")).execute();
      databaseDocumentTx.command(new OCommandSQL("insert into Foo set bar = ['yep']")).execute();
      List<ODocument> results = databaseDocumentTx.command(new OCommandSQL("select from Foo where bar = 'yep'")).execute();
      assertEquals(results.size(), 1);
      results = databaseDocumentTx.command(new OCommandSQL("select from index:Foo.bar where key = 'yep'")).execute();
      assertEquals(results.size(), 1);
    } finally {
      databaseDocumentTx.drop();
    }
  }

}
