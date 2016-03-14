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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Test
public class TruncateClassTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public TruncateClassTest(@Optional String url) {
    super(url);
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

  @Test
  public void testTruncateVertexClass() {

    if (!database.getMetadata().getSchema().existsClass("V"))
      database.getMetadata().getSchema().createClass("V");
    if (!database.getMetadata().getSchema().existsClass("E"))
      database.getMetadata().getSchema().createClass("E");

    database.command(new OCommandSQL("create class TestTruncateVertexClass extends V")).execute();
    database.command(new OCommandSQL("create vertex TestTruncateVertexClass set name = 'foo'")).execute();

    try {
      database.command(new OCommandSQL("truncate class TestTruncateVertexClass ")).execute();
      Assert.fail();
    } catch (Exception e) {
    }
    List<?> result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClass"));
    Assert.assertEquals(result.size(), 1);

    database.command(new OCommandSQL("truncate class TestTruncateVertexClass unsafe")).execute();
    result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClass"));
    Assert.assertEquals(result.size(), 0);

  }

  @Test
  public void testTruncateVertexClassSubclasses() {

    database.command(new OCommandSQL("create class TestTruncateVertexClassSuperclass")).execute();
    database.command(new OCommandSQL("create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass"))
        .execute();

    database.command(new OCommandSQL("insert into TestTruncateVertexClassSuperclass set name = 'foo'")).execute();
    database.command(new OCommandSQL("insert into TestTruncateVertexClassSubclass set name = 'bar'")).execute();

    List<?> result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSuperclass"));
    Assert.assertEquals(result.size(), 2);

    database.command(new OCommandSQL("truncate class TestTruncateVertexClassSuperclass ")).execute();
    result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSubclass"));
    Assert.assertEquals(result.size(), 1);

    database.command(new OCommandSQL("truncate class TestTruncateVertexClassSuperclass polymorphic")).execute();
    result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSubclass"));
    Assert.assertEquals(result.size(), 0);

  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {

    database.command(new OCommandSQL("create class TestTruncateVertexClassSuperclassWithIndex")).execute();
    database.command(new OCommandSQL("create property TestTruncateVertexClassSuperclassWithIndex.name STRING")).execute();
    database
        .command(
            new OCommandSQL(
                "create index TestTruncateVertexClassSuperclassWithIndex_index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE"))
        .execute();

    database
        .command(
            new OCommandSQL(
                "create class TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex"))
        .execute();

    database.command(new OCommandSQL("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'")).execute();
    database.command(new OCommandSQL("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'")).execute();

    List<?> result = database.query(new OSQLSynchQuery("select from index:TestTruncateVertexClassSuperclassWithIndex_index"));
    Assert.assertEquals(result.size(), 2);

    database.command(new OCommandSQL("truncate class TestTruncateVertexClassSubclassWithIndex")).execute();
    result = database.query(new OSQLSynchQuery("select from index:TestTruncateVertexClassSuperclassWithIndex_index"));
    Assert.assertEquals(result.size(), 1);

    database.command(new OCommandSQL("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic")).execute();
    result = database.query(new OSQLSynchQuery("select from index:TestTruncateVertexClassSuperclassWithIndex_index"));
    Assert.assertEquals(result.size(), 0);

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
