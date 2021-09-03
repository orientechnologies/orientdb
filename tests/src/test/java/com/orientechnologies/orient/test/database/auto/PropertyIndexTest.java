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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class PropertyIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public PropertyIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("PropertyIndexTestClass");
    oClass.createProperty("prop0", OType.LINK);
    oClass.createProperty("prop1", OType.STRING);
    oClass.createProperty("prop2", OType.INTEGER);
    oClass.createProperty("prop3", OType.BOOLEAN);
    oClass.createProperty("prop4", OType.INTEGER);
    oClass.createProperty("prop5", OType.STRING);
  }

  @AfterClass
  public void afterClass() {
    if (database.isClosed()) database.open("admin", "admin");

    database.command(new OCommandSQL("delete from PropertyIndexTestClass")).execute();
    database.command(new OCommandSQL("drop class PropertyIndexTestClass")).execute();
    database.reload();
    database.close();
  }

  @Test
  public void testCreateUniqueIndex() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");
    final OProperty propOne = oClass.getProperty("prop1");

    propOne.createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));

    final Collection<OIndex> indexes = propOne.getIndexes();
    OIndexDefinition indexDefinition = null;

    for (final OIndex index : indexes) {
      if (index.getName().equals("PropertyIndexTestClass.prop1")) {
        indexDefinition = index.getDefinition();
        break;
      }
    }

    Assert.assertNotNull(indexDefinition);
    Assert.assertEquals(indexDefinition.getParamCount(), 1);
    Assert.assertEquals(indexDefinition.getFields().size(), 1);
    Assert.assertTrue(indexDefinition.getFields().contains("prop1"));
    Assert.assertEquals(indexDefinition.getTypes().length, 1);
    Assert.assertEquals(indexDefinition.getTypes()[0], OType.STRING);
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void createAdditionalSchemas() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(
        "propOne0",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop0", "prop1"});
    oClass.createIndex(
        "propOne1",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop1", "prop2"});
    oClass.createIndex(
        "propOne2",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop1", "prop3"});
    oClass.createIndex(
        "propOne3",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop2", "prop3"});
    oClass.createIndex(
        "propOne4",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop2", "prop1"});
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetIndexes() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");
    final OProperty propOne = oClass.getProperty("prop1");

    final Collection<OIndex> indexes = propOne.getIndexes();
    Assert.assertEquals(indexes.size(), 1);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetAllIndexes() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");
    final OProperty propOne = oClass.getProperty("prop1");

    final Collection<OIndex> indexes = propOne.getAllIndexes();
    Assert.assertEquals(indexes.size(), 5);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne0"));
    Assert.assertNotNull(containsIndex(indexes, "propOne1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne2"));
    Assert.assertNotNull(containsIndex(indexes, "propOne4"));
  }

  @Test
  public void testIsIndexedNonIndexedField() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");
    final OProperty propThree = oClass.getProperty("prop3");
    Assert.assertFalse(propThree.isIndexed());
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void testIsIndexedIndexedField() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");
    final OProperty propOne = oClass.getProperty("prop1");
    Assert.assertTrue(propOne.isIndexed());
  }

  @Test(dependsOnMethods = {"testIsIndexedIndexedField"})
  public void testIndexingCompositeRIDAndOthers() throws Exception {
    checkEmbeddedDB();

    long prev0 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size();
    long prev1 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size();

    ODocument doc =
        new ODocument("PropertyIndexTestClass").fields("prop1", "testComposite3").save();
    new ODocument("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite1").save();
    new ODocument("PropertyIndexTestClass").fields("prop0", doc).save();

    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(),
        prev0 + 1);
    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(),
        prev1);
  }

  @Test(dependsOnMethods = {"testIndexingCompositeRIDAndOthers"})
  public void testIndexingCompositeRIDAndOthersInTx() throws Exception {
    database.begin();

    long prev0 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size();
    long prev1 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size();

    ODocument doc =
        new ODocument("PropertyIndexTestClass").fields("prop1", "testComposite34").save();
    new ODocument("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite33").save();
    new ODocument("PropertyIndexTestClass").fields("prop0", doc).save();

    database.commit();

    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(),
        prev0 + 1);
    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(),
        prev1);
  }

  @Test
  public void testDropIndexes() throws Exception {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(
        "PropertyIndexFirstIndex",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop4"});
    oClass.createIndex(
        "PropertyIndexSecondIndex",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop4"});

    oClass.getProperty("prop4").dropIndexes();

    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexFirstIndex"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexSecondIndex"));
  }

  @Test
  public void testDropIndexesForComposite() throws Exception {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(
        "PropertyIndexFirstIndex",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop4"});
    oClass.createIndex(
        "PropertyIndexSecondIndex",
        OClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"prop4", "prop5"});

    try {
      oClass.getProperty("prop4").dropIndexes();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(
          e.getMessage().contains("This operation applicable only for property indexes. "));
    }

    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexFirstIndex"));
    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexSecondIndex"));
  }

  private OIndex containsIndex(final Collection<OIndex> indexes, final String indexName) {
    for (final OIndex index : indexes) {
      if (index.getName().equals(indexName)) return index;
    }
    return null;
  }
}
