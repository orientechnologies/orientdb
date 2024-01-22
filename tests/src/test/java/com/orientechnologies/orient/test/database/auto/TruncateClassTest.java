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
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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

    database.command("truncate class test_class").close();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    database.command("truncate class test_class").close();

    database.save(
        new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    database.save(
        new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    List<OResult> result =
        database.query("select from test_class").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    Set<Integer> set = new HashSet<Integer>();
    for (OResult document : result) {
      set.addAll((Collection<Integer>) document.getProperty("data"));
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
    database.command("create class TestTruncateVertexClass extends V").close();
    database.command("create vertex TestTruncateVertexClass set name = 'foo'").close();

    try {
      database.command("truncate class TestTruncateVertexClass ").close();
      Assert.fail();
    } catch (Exception e) {
    }
    OResultSet result = database.query("select from TestTruncateVertexClass");
    Assert.assertEquals(result.stream().count(), 1);

    database.command("truncate class TestTruncateVertexClass unsafe").close();
    result = database.query("select from TestTruncateVertexClass");
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testTruncateVertexClassSubclasses() {

    database.command("create class TestTruncateVertexClassSuperclass").close();
    database
        .command(
            "create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass")
        .close();

    database.command("insert into TestTruncateVertexClassSuperclass set name = 'foo'").close();
    database.command("insert into TestTruncateVertexClassSubclass set name = 'bar'").close();

    OResultSet result = database.query("select from TestTruncateVertexClassSuperclass");
    Assert.assertEquals(result.stream().count(), 2);

    database.command("truncate class TestTruncateVertexClassSuperclass ").close();
    result = database.query("select from TestTruncateVertexClassSubclass");
    Assert.assertEquals(result.stream().count(), 1);

    database.command("truncate class TestTruncateVertexClassSuperclass polymorphic").close();
    result = database.query("select from TestTruncateVertexClassSubclass");
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {
    checkEmbeddedDB();

    database.command("create class TestTruncateVertexClassSuperclassWithIndex").close();
    database
        .command("create property TestTruncateVertexClassSuperclassWithIndex.name STRING")
        .close();
    database
        .command(
            "create index TestTruncateVertexClassSuperclassWithIndex_index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE")
        .close();

    database
        .command(
            "create class TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex")
        .close();

    database
        .command("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'")
        .close();
    database
        .command("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'")
        .close();

    final OIndex index = getIndex("TestTruncateVertexClassSuperclassWithIndex_index");
    Assert.assertEquals(index.getInternal().size(), 2);

    database.command("truncate class TestTruncateVertexClassSubclassWithIndex").close();
    Assert.assertEquals(index.getInternal().size(), 1);

    database
        .command("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic")
        .close();
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

    database.command("truncate class test_class").close();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    OResultSet result = database.query("select from test_class");
    Assert.assertEquals(result.stream().count(), 2);

    database.command("truncate class test_class").close();

    result = database.query("select from test_class");
    Assert.assertEquals(result.stream().count(), 0);

    schema.dropClass("test_class");
  }
}
