/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.*;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class TruncateTest {
  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public TruncateTest(@Optional String iURL) {
    final String url = iURL != null ? iURL : "memory:test";
    database = new ODatabaseDocumentTx(url);
  }

  @BeforeMethod
  public void openDatabase() {
    if (database.getURL().startsWith(OEngineMemory.NAME) && !database.exists())
      database.create();
    else
      database.open("admin", "admin");
  }

  @AfterMethod
  public void closeDatabase() {
    database.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    final OIndex<?> index = getOrCreateIndex(testClass);
    schema.save();

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from test_class"));
    Assert.assertEquals(result.size(), 2);
    Set<Integer> set = new HashSet<Integer>();
    for (ODocument document : result) {
      set.addAll((Collection<Integer>) document.field("data"));
    }
    Assert.assertTrue(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1)));

    Assert.assertEquals(index.getSize(), 6);

    OIndexCursor cursor = index.cursor();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      Assert.assertTrue(set.contains((Integer) entry.getKey()));
      entry = cursor.nextEntry();
    }

    schema.dropClass("test_class");
  }

  private OIndex<?> getOrCreateIndex(OClass testClass) {
    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("test_class_by_data");
    if (index == null) {
      testClass.createProperty("data", OType.EMBEDDEDLIST, OType.INTEGER);
      index = testClass.createIndex("test_class_by_data", OClass.INDEX_TYPE.UNIQUE, "data");
    }
    return index;
  }

  private OClass getOrCreateClass(OSchema schema) {
    OClass testClass;
    if (schema.existsClass("test_class")) {
      testClass = schema.getClass("test_class");
    } else {
      testClass = schema.createClass("test_class");
    }
    schema.save();
    return testClass;
  }
}
