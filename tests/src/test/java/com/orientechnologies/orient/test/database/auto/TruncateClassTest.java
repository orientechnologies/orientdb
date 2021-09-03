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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TruncateClassTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public TruncateClassTest(@Optional String url) {
    super(url);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {
    checkEmbeddedDB();

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    final OIndex index = getOrCreateIndex(testClass);

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(
        new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    database.save(
        new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    List<ODocument> result =
        database.query(new OSQLSynchQuery<ODocument>("select from test_class"));
    Assert.assertEquals(result.size(), 2);
    Set<Integer> set = new HashSet<Integer>();
    for (ODocument document : result) {
      set.addAll((Collection<Integer>) document.field("data"));
    }
    Assert.assertTrue(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1)));

    Assert.assertEquals(index.getInternal().size(), 6);

    Iterator<ORawPair<Object, ORID>> indexIterator;
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      indexIterator = stream.iterator();

      while (indexIterator.hasNext()) {
        ORawPair<Object, ORID> entry = indexIterator.next();
        Assert.assertTrue(set.contains((Integer) entry.first));
      }
    }

    schema.dropClass("test_class");
  }

  @Test
  public void testTruncateVertexClass() {
    database.command(new OCommandSQL("create class TestTruncateVertexClass extends V")).execute();
    database
        .command(new OCommandSQL("create vertex TestTruncateVertexClass set name = 'foo'"))
        .execute();

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
    database
        .command(
            new OCommandSQL(
                "create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass"))
        .execute();

    database
        .command(new OCommandSQL("insert into TestTruncateVertexClassSuperclass set name = 'foo'"))
        .execute();
    database
        .command(new OCommandSQL("insert into TestTruncateVertexClassSubclass set name = 'bar'"))
        .execute();

    List<?> result =
        database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSuperclass"));
    Assert.assertEquals(result.size(), 2);

    database
        .command(new OCommandSQL("truncate class TestTruncateVertexClassSuperclass "))
        .execute();
    result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSubclass"));
    Assert.assertEquals(result.size(), 1);

    database
        .command(new OCommandSQL("truncate class TestTruncateVertexClassSuperclass polymorphic"))
        .execute();
    result = database.query(new OSQLSynchQuery("select from TestTruncateVertexClassSubclass"));
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {
    checkEmbeddedDB();

    database
        .command(new OCommandSQL("create class TestTruncateVertexClassSuperclassWithIndex"))
        .execute();
    database
        .command(
            new OCommandSQL(
                "create property TestTruncateVertexClassSuperclassWithIndex.name STRING"))
        .execute();
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

    database
        .command(
            new OCommandSQL(
                "insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'"))
        .execute();
    database
        .command(
            new OCommandSQL(
                "insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'"))
        .execute();

    final OIndex index = getIndex("TestTruncateVertexClassSuperclassWithIndex_index");
    Assert.assertEquals(index.getInternal().size(), 2);

    database
        .command(new OCommandSQL("truncate class TestTruncateVertexClassSubclassWithIndex"))
        .execute();
    Assert.assertEquals(index.getInternal().size(), 1);

    database
        .command(
            new OCommandSQL(
                "truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic"))
        .execute();
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  private OIndex getOrCreateIndex(OClass testClass) {
    OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, "test_class_by_data");
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
    return testClass;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClassWithCommandCache() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);
    boolean ccWasEnabled = false;

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    List<ODocument> result =
        database.query(new OSQLSynchQuery<ODocument>("select from test_class"));
    Assert.assertEquals(result.size(), 2);

    database.command(new OCommandSQL("truncate class test_class")).execute();

    result = database.query(new OSQLSynchQuery<ODocument>("select from test_class"));
    Assert.assertEquals(result.size(), 0);

    schema.dropClass("test_class");
  }
}
