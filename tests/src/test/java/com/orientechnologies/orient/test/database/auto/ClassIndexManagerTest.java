package com.orientechnologies.orient.test.database.auto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

@Test(groups = { "index" })
public class ClassIndexManagerTest {
  private final ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public ClassIndexManagerTest(final String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeClass
  public void beforeClass() {
    if (database.isClosed())
      database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    final OClass superClass = schema.createClass("classIndexManagerTestSuperClass");
    final OProperty propertyZero = superClass.createProperty("prop0", OType.STRING);
    propertyZero.createIndex(OClass.INDEX_TYPE.UNIQUE);

    final OClass oClass = schema.createClass("classIndexManagerTestClass", superClass);
    final OProperty propOne = oClass.createProperty("prop1", OType.STRING);
    propOne.createIndex(OClass.INDEX_TYPE.UNIQUE);

    final OProperty propTwo = oClass.createProperty("prop2", OType.INTEGER);
    propTwo.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createProperty("prop3", OType.BOOLEAN);

    final OProperty propFour = oClass.createProperty("prop4", OType.EMBEDDEDLIST, OType.STRING);
    propFour.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createProperty("prop5", OType.EMBEDDEDMAP, OType.STRING);
    oClass.createIndex("classIndexManagerTestIndexByKey", OClass.INDEX_TYPE.NOTUNIQUE, "prop5");
    oClass.createIndex("classIndexManagerTestIndexByValue", OClass.INDEX_TYPE.NOTUNIQUE, "prop5 by value");

    final OProperty propSix = oClass.createProperty("prop6", OType.EMBEDDEDSET, OType.STRING);
    propSix.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    oClass.createIndex("classIndexManagerComposite", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

    final OClass oClassTwo = schema.createClass("classIndexManagerTestClassTwo");
    oClassTwo.createProperty("prop1", OType.STRING);
    oClassTwo.createProperty("prop2", OType.INTEGER);

    final OClass compositeCollectionClass = schema.createClass("classIndexManagerTestCompositeCollectionClass");
    compositeCollectionClass.createProperty("prop1", OType.STRING);
    compositeCollectionClass.createProperty("prop2", OType.EMBEDDEDLIST, OType.INTEGER);

    compositeCollectionClass
        .createIndex("classIndexManagerTestIndexValueAndCollection", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

    oClass.createIndex("classIndexManagerTestIndexOnPropertiesFromClassAndSuperclass", OClass.INDEX_TYPE.UNIQUE, "prop0", "prop1");

    schema.save();

    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    if (database.isClosed())
      database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    database.command(new OCommandSQL("delete from classIndexManagerTestClass")).execute();
    database.command(new OCommandSQL("delete from classIndexManagerTestClassTwo")).execute();
    database.command(new OCommandSQL("delete from classIndexManagerTestSuperClass")).execute();
    database.close();
  }

  @AfterClass
  public void afterClass() {
    if (database.isClosed())
      database.open("admin", "admin");
    database.command(new OCommandSQL("drop class classIndexManagerTestClass")).execute();
    database.command(new OCommandSQL("drop class classIndexManagerTestClassTwo")).execute();
    database.command(new OCommandSQL("drop class classIndexManagerTestSuperClass")).execute();
    database.getMetadata().getSchema().reload();
    database.getLevel2Cache().clear();
    database.close();
  }

  public void testPropertiesCheckUniqueIndexDubKeysCreate() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    docOne.field("prop1", "a");
    docOne.save();

    boolean exceptionThrown = false;
    try {
      docTwo.field("prop1", "a");
      docTwo.save();
    } catch (ORecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullCreate() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullCreateInTx() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    database.begin();
    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckUniqueIndexInParentDubKeysCreate() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    docOne.field("prop0", "a");
    docOne.save();

    boolean exceptionThrown = false;
    try {
      docTwo.field("prop0", "a");
      docTwo.save();
    } catch (ORecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeysUpdate() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    boolean exceptionThrown = false;
    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", "b");
    docTwo.save();

    try {
      docTwo.field("prop1", "a");
      docTwo.save();
    } catch (ORecordDuplicatedException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullUpdate() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", "b");
    docTwo.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
  }

  public void testPropertiesCheckUniqueIndexDubKeyIsNullUpdateInTX() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    final ODocument docTwo = new ODocument("classIndexManagerTestClass");

    database.begin();
    docOne.field("prop1", "a");
    docOne.save();

    docTwo.field("prop1", "b");
    docTwo.save();

    docTwo.field("prop1", (String) null);
    docTwo.save();
    database.commit();
  }

  public void testPropertiesCheckNonUniqueIndexDubKeys() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    docOne.field("prop2", 1);
    docOne.save();

    final ODocument docTwo = new ODocument("classIndexManagerTestClass");
    docTwo.field("prop2", 1);
    docTwo.save();
  }

  public void testPropertiesCheckUniqueNullKeys() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    docOne.save();

    final ODocument docTwo = new ODocument("classIndexManagerTestClass");
    docTwo.save();
  }

  public void testCreateDocumentWithoutClass() {
    final Collection<? extends OIndex<?>> beforeIndexes = database.getMetadata().getIndexManager().getIndexes();
    final Map<String, Long> indexSizeMap = new HashMap<String, Long>();

    for (final OIndex<?> index : beforeIndexes)
      indexSizeMap.put(index.getName(), index.getSize());

    final ODocument docOne = new ODocument();
    docOne.field("prop1", "a");
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.field("prop1", "a");
    docTwo.save();

    final Collection<? extends OIndex<?>> afterIndexes = database.getMetadata().getIndexManager().getIndexes();
    for (final OIndex<?> index : afterIndexes)
      Assert.assertEquals(index.getSize(), indexSizeMap.get(index.getName()).longValue());
  }

  public void testUpdateDocumentWithoutClass() {
    final Collection<? extends OIndex<?>> beforeIndexes = database.getMetadata().getIndexManager().getIndexes();
    final Map<String, Long> indexSizeMap = new HashMap<String, Long>();

    for (final OIndex<?> index : beforeIndexes)
      indexSizeMap.put(index.getName(), index.getSize());

    final ODocument docOne = new ODocument();
    docOne.field("prop1", "a");
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.field("prop1", "b");
    docTwo.save();

    docOne.field("prop1", "a");
    docOne.save();

    final Collection<? extends OIndex<?>> afterIndexes = database.getMetadata().getIndexManager().getIndexes();
    for (final OIndex<?> index : afterIndexes)
      Assert.assertEquals(index.getSize(), indexSizeMap.get(index.getName()).longValue());
  }

  public void testDeleteDocumentWithoutClass() {
    final ODocument docOne = new ODocument();
    docOne.field("prop1", "a");
    docOne.save();

    docOne.delete();
  }

  public void testDeleteModifiedDocumentWithoutClass() {
    final ODocument docOne = new ODocument();
    docOne.field("prop1", "a");
    docOne.save();

    docOne.field("prop1", "b");

    docOne.delete();
  }

  public void testDocumentUpdateWithoutDirtyFields() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    docOne.field("prop1", "a");
    docOne.save();

    docOne.setDirty();
    docOne.save();
  }

  public void testCreateDocumentIndexRecordAdded() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    Assert.assertNotNull(propOneIndex.get("a"));
    Assert.assertEquals(propOneIndex.getSize(), 1);

    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

    final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();
    Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 1)));
    Assert.assertEquals(compositeIndex.getSize(), 1);

    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
    Assert.assertNotNull(propZeroIndex.get("x"));
    Assert.assertEquals(propZeroIndex.getSize(), 1);
  }

  public void testUpdateDocumentIndexRecordRemoved() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);
    Assert.assertEquals(propZeroIndex.getSize(), 1);

    doc.removeField("prop2");
    doc.removeField("prop0");
    doc.save();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 0);
    Assert.assertEquals(propZeroIndex.getSize(), 0);
  }

  public void testUpdateDocumentNullKeyIndexRecordRemoved() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");

    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);
    Assert.assertEquals(propZeroIndex.getSize(), 1);

    doc.field("prop2", (Object) null);
    doc.field("prop0", (Object) null);
    doc.save();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 0);
    Assert.assertEquals(propZeroIndex.getSize(), 0);
  }

  public void testUpdateDocumentIndexRecordUpdated() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
    final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);
    Assert.assertEquals(propZeroIndex.getSize(), 1);

    doc.field("prop2", 2);
    doc.field("prop0", "y");
    doc.save();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);
    Assert.assertEquals(propZeroIndex.getSize(), 1);

    Assert.assertNotNull(propZeroIndex.get("y"));
    Assert.assertNotNull(propOneIndex.get("a"));
    Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 2)));
  }

  public void testUpdateDocumentIndexRecordUpdatedFromNullField() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");
    final OIndexDefinition compositeIndexDefinition = compositeIndex.getDefinition();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 0);

    doc.field("prop2", 2);
    doc.save();

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);

    Assert.assertNotNull(propOneIndex.get("a"));
    Assert.assertNotNull(compositeIndex.get(compositeIndexDefinition.createValue("a", 2)));
  }

  public void testListUpdate() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propFourIndex = oClass.getClassIndex("classIndexManagerTestClass.prop4");

    Assert.assertEquals(propFourIndex.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final List<String> listProperty = new ArrayList<String>();
    listProperty.add("value1");
    listProperty.add("value2");

    doc.field("prop4", listProperty);
    doc.save();

    Assert.assertEquals(propFourIndex.getSize(), 2);
    Assert.assertNotNull(propFourIndex.get("value1"));
    Assert.assertNotNull(propFourIndex.get("value2"));

    List<String> trackedList = doc.field("prop4");
    trackedList.set(0, "value3");

    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.remove("value4");
    trackedList.remove("value2");
    trackedList.add("value5");

    doc.save();

    Assert.assertEquals(propFourIndex.getSize(), 3);
    Assert.assertNotNull(propFourIndex.get("value3"));
    Assert.assertNotNull(propFourIndex.get("value4"));
    Assert.assertNotNull(propFourIndex.get("value5"));
  }

  public void testMapUpdate() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propFiveIndexKey = oClass.getClassIndex("classIndexManagerTestIndexByKey");
    final OIndex<?> propFiveIndexValue = oClass.getClassIndex("classIndexManagerTestIndexByValue");

    Assert.assertEquals(propFiveIndexKey.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final Map<String, String> mapProperty = new HashMap<String, String>();
    mapProperty.put("key1", "value1");
    mapProperty.put("key2", "value2");

    doc.field("prop5", mapProperty);
    doc.save();

    Assert.assertEquals(propFiveIndexKey.getSize(), 2);
    Assert.assertNotNull(propFiveIndexKey.get("key1"));
    Assert.assertNotNull(propFiveIndexKey.get("key2"));

    Map<String, String> trackedMap = doc.field("prop5");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.remove("key1");
    trackedMap.put("key1", "value5");
    trackedMap.remove("key2");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value6");
    trackedMap.put("key8", "value6");
    trackedMap.put("key4", "value7");

    trackedMap.remove("key8");

    doc.save();

    Assert.assertEquals(propFiveIndexKey.getSize(), 5);
    Assert.assertNotNull(propFiveIndexKey.get("key1"));
    Assert.assertNotNull(propFiveIndexKey.get("key3"));
    Assert.assertNotNull(propFiveIndexKey.get("key4"));
    Assert.assertNotNull(propFiveIndexKey.get("key6"));
    Assert.assertNotNull(propFiveIndexKey.get("key7"));

    Assert.assertEquals(propFiveIndexValue.getSize(), 4);
    Assert.assertNotNull(propFiveIndexValue.get("value5"));
    Assert.assertNotNull(propFiveIndexValue.get("value3"));
    Assert.assertNotNull(propFiveIndexValue.get("value7"));
    Assert.assertNotNull(propFiveIndexValue.get("value6"));

  }

  public void testSetUpdate() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propSixIndex = oClass.getClassIndex("classIndexManagerTestClass.prop6");

    Assert.assertEquals(propSixIndex.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final Set<String> setProperty = new HashSet<String>();
    setProperty.add("value1");
    setProperty.add("value2");

    doc.field("prop6", setProperty);
    doc.save();

    Assert.assertEquals(propSixIndex.getSize(), 2);
    Assert.assertNotNull(propSixIndex.get("value1"));
    Assert.assertNotNull(propSixIndex.get("value2"));

    Set<String> trackedSet = doc.field("prop6");

    trackedSet.add("value4");
    trackedSet.add("value4");
    trackedSet.add("value4");
    trackedSet.remove("value4");
    trackedSet.remove("value2");
    trackedSet.add("value5");

    doc.save();

    Assert.assertEquals(propSixIndex.getSize(), 2);
    Assert.assertNotNull(propSixIndex.get("value1"));
    Assert.assertNotNull(propSixIndex.get("value5"));
  }

  public void testListDelete() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propFourIndex = oClass.getClassIndex("classIndexManagerTestClass.prop4");

    Assert.assertEquals(propFourIndex.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final List<String> listProperty = new ArrayList<String>();
    listProperty.add("value1");
    listProperty.add("value2");

    doc.field("prop4", listProperty);
    doc.save();

    Assert.assertEquals(propFourIndex.getSize(), 2);
    Assert.assertNotNull(propFourIndex.get("value1"));
    Assert.assertNotNull(propFourIndex.get("value2"));

    List<String> trackedList = doc.field("prop4");
    trackedList.set(0, "value3");

    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.add("value4");
    trackedList.remove("value4");
    trackedList.remove("value2");
    trackedList.add("value5");

    doc.save();

    Assert.assertEquals(propFourIndex.getSize(), 3);
    Assert.assertNotNull(propFourIndex.get("value3"));
    Assert.assertNotNull(propFourIndex.get("value4"));
    Assert.assertNotNull(propFourIndex.get("value5"));

    trackedList = doc.field("prop4");
    trackedList.remove("value3");
    trackedList.remove("value4");
    trackedList.add("value8");

    doc.delete();

    Assert.assertEquals(propFourIndex.getSize(), 0);
  }

  public void testMapDelete() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propFiveIndexKey = oClass.getClassIndex("classIndexManagerTestIndexByKey");
    final OIndex<?> propFiveIndexValue = oClass.getClassIndex("classIndexManagerTestIndexByValue");

    Assert.assertEquals(propFiveIndexKey.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final Map<String, String> mapProperty = new HashMap<String, String>();
    mapProperty.put("key1", "value1");
    mapProperty.put("key2", "value2");

    doc.field("prop5", mapProperty);
    doc.save();

    Assert.assertEquals(propFiveIndexKey.getSize(), 2);
    Assert.assertNotNull(propFiveIndexKey.get("key1"));
    Assert.assertNotNull(propFiveIndexKey.get("key2"));

    Map<String, String> trackedMap = doc.field("prop5");
    trackedMap.put("key3", "value3");
    trackedMap.put("key4", "value4");
    trackedMap.remove("key1");
    trackedMap.put("key1", "value5");
    trackedMap.remove("key2");
    trackedMap.put("key6", "value6");
    trackedMap.put("key7", "value6");
    trackedMap.put("key8", "value6");
    trackedMap.put("key4", "value7");

    trackedMap.remove("key8");

    doc.save();

    Assert.assertEquals(propFiveIndexKey.getSize(), 5);
    Assert.assertNotNull(propFiveIndexKey.get("key1"));
    Assert.assertNotNull(propFiveIndexKey.get("key3"));
    Assert.assertNotNull(propFiveIndexKey.get("key4"));
    Assert.assertNotNull(propFiveIndexKey.get("key6"));
    Assert.assertNotNull(propFiveIndexKey.get("key7"));

    Assert.assertEquals(propFiveIndexValue.getSize(), 4);
    Assert.assertNotNull(propFiveIndexValue.get("value5"));
    Assert.assertNotNull(propFiveIndexValue.get("value3"));
    Assert.assertNotNull(propFiveIndexValue.get("value7"));
    Assert.assertNotNull(propFiveIndexValue.get("value6"));

    trackedMap = doc.field("prop5");

    trackedMap.remove("key1");
    trackedMap.remove("key3");
    trackedMap.remove("key4");
    trackedMap.put("key6", "value10");
    trackedMap.put("key11", "value11");

    doc.delete();

    Assert.assertEquals(propFiveIndexKey.getSize(), 0);
    Assert.assertEquals(propFiveIndexValue.getSize(), 0);
  }

  public void testSetDelete() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propSixIndex = oClass.getClassIndex("classIndexManagerTestClass.prop6");

    Assert.assertEquals(propSixIndex.getSize(), 0);

    final ODocument doc = new ODocument("classIndexManagerTestClass");

    final Set<String> setProperty = new HashSet<String>();
    setProperty.add("value1");
    setProperty.add("value2");

    doc.field("prop6", setProperty);
    doc.save();

    Assert.assertEquals(propSixIndex.getSize(), 2);
    Assert.assertNotNull(propSixIndex.get("value1"));
    Assert.assertNotNull(propSixIndex.get("value2"));

    Set<String> trackedSet = doc.field("prop6");

    trackedSet.add("value4");
    trackedSet.add("value4");
    trackedSet.add("value4");
    trackedSet.remove("value4");
    trackedSet.remove("value2");
    trackedSet.add("value5");

    doc.save();

    Assert.assertEquals(propSixIndex.getSize(), 2);
    Assert.assertNotNull(propSixIndex.get("value1"));
    Assert.assertNotNull(propSixIndex.get("value5"));

    trackedSet = doc.field("prop6");
    trackedSet.remove("value1");
    trackedSet.add("value6");

    doc.delete();

    Assert.assertEquals(propSixIndex.getSize(), 0);
  }

  public void testDeleteDocumentIndexRecordDeleted() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

    Assert.assertEquals(propZeroIndex.getSize(), 1);
    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);

    doc.delete();

    Assert.assertEquals(propZeroIndex.getSize(), 0);
    Assert.assertEquals(propOneIndex.getSize(), 0);
    Assert.assertEquals(compositeIndex.getSize(), 0);
  }

  public void testDeleteUpdatedDocumentIndexRecordDeleted() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop0", "x");
    doc.field("prop1", "a");
    doc.field("prop2", 1);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oSuperClass = schema.getClass("classIndexManagerTestSuperClass");
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

    final OIndex<?> propZeroIndex = oSuperClass.getClassIndex("classIndexManagerTestSuperClass.prop0");
    Assert.assertEquals(propZeroIndex.getSize(), 1);
    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 1);

    doc.field("prop2", 2);
    doc.field("prop0", "y");

    doc.delete();

    Assert.assertEquals(propZeroIndex.getSize(), 0);
    Assert.assertEquals(propOneIndex.getSize(), 0);
    Assert.assertEquals(compositeIndex.getSize(), 0);
  }

  public void testDeleteUpdatedDocumentNullFieldIndexRecordDeleted() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 0);

    doc.delete();

    Assert.assertEquals(propOneIndex.getSize(), 0);
    Assert.assertEquals(compositeIndex.getSize(), 0);
  }

  public void testDeleteUpdatedDocumentOrigNullFieldIndexRecordDeleted() {
    final ODocument doc = new ODocument("classIndexManagerTestClass");
    doc.field("prop1", "a");
    doc.field("prop2", (Object) null);

    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final OIndex<?> propOneIndex = oClass.getClassIndex("classIndexManagerTestClass.prop1");
    final OIndex<?> compositeIndex = oClass.getClassIndex("classIndexManagerComposite");

    Assert.assertEquals(propOneIndex.getSize(), 1);
    Assert.assertEquals(compositeIndex.getSize(), 0);

    doc.field("prop2", 2);

    doc.delete();

    Assert.assertEquals(propOneIndex.getSize(), 0);
    Assert.assertEquals(compositeIndex.getSize(), 0);
  }

  public void testNoClassIndexesUpdate() {
    final ODocument doc = new ODocument("classIndexManagerTestClassTwo");
    doc.field("prop1", "a");
    doc.save();

    doc.field("prop1", "b");
    doc.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");

    final Collection<OIndex<?>> indexes = oClass.getIndexes();
    for (final OIndex<?> index : indexes) {
      Assert.assertEquals(index.getSize(), 0);
    }
  }

  public void testNoClassIndexesDelete() {
    final ODocument doc = new ODocument("classIndexManagerTestClassTwo");
    doc.field("prop1", "a");
    doc.save();

    doc.delete();
  }

  public void testCollectionCompositeCreation() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    Assert.assertEquals(index.get(new OCompositeKey("test1", 1)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test1", 2)), doc.getIdentity());

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeNullSimpleFieldCreation() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", (Object) null);
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 0);

    doc.delete();
  }

  public void testCollectionCompositeNullCollectionFieldCreation() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", (Object) null);

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 0);

    doc.delete();
  }

  public void testCollectionCompositeUpdateSimpleField() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop1", "test2");

    doc.save();

    Assert.assertEquals(index.get(new OCompositeKey("test2", 1)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test2", 2)), doc.getIdentity());

    Assert.assertEquals(index.getSize(), 2);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasAssigned() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", Arrays.asList(1, 3));

    doc.save();

    Assert.assertEquals(index.get(new OCompositeKey("test1", 1)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test1", 3)), doc.getIdentity());

    Assert.assertEquals(index.getSize(), 2);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChanged() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.save();

    Assert.assertEquals(index.get(new OCompositeKey("test1", 2)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test1", 3)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test1", 4)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test1", 5)), doc.getIdentity());

    Assert.assertEquals(index.getSize(), 4);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChangedSimpleFieldWasAssigned() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.field("prop1", "test2");

    doc.save();

    Assert.assertEquals(index.getSize(), 4);

    Assert.assertEquals(index.get(new OCompositeKey("test2", 2)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test2", 3)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test2", 4)), doc.getIdentity());
    Assert.assertEquals(index.get(new OCompositeKey("test2", 5)), doc.getIdentity());

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateSimpleFieldNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop1", (Object) null);

    doc.save();

    Assert.assertEquals(index.getSize(), 0);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasAssignedNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", (Object) null);

    doc.save();

    Assert.assertEquals(index.getSize(), 0);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateBothAssignedNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", (Object) null);
    doc.field("prop1", (Object) null);

    doc.save();

    Assert.assertEquals(index.getSize(), 0);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeUpdateCollectionWasChangedSimpleFieldWasAssignedNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);
    docList.add(5);

    docList.remove(0);

    doc.field("prop1", (Object) null);

    doc.save();

    Assert.assertEquals(index.getSize(), 0);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteSimpleFieldAssigend() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop1", "test2");
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldAssigend() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", Arrays.asList(1, 3));
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldChanged() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteBothCollectionSimpleFieldChanged() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.field("prop1", "test2");

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteBothCollectionSimpleFieldAssigend() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", Arrays.asList(1, 3));
    doc.field("prop1", "test2");
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteSimpleFieldNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop1", (Object) null);
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", (Object) null);
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteBothSimpleCollectionFieldNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    doc.field("prop2", (Object) null);
    doc.field("prop1", (Object) null);
    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testCollectionCompositeDeleteCollectionFieldChangedSimpleFieldNull() {
    final ODocument doc = new ODocument("classIndexManagerTestCompositeCollectionClass");

    doc.field("prop1", "test1");
    doc.field("prop2", Arrays.asList(1, 2));

    doc.save();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("classIndexManagerTestIndexValueAndCollection");
    Assert.assertEquals(index.getSize(), 2);

    List<Integer> docList = doc.field("prop2");
    docList.add(3);
    docList.add(4);

    docList.remove(1);

    doc.field("prop1", (Object) null);

    doc.delete();

    Assert.assertEquals(index.getSize(), 0);
  }

  public void testIndexOnPropertiesFromClassAndSuperclass() {
    final ODocument docOne = new ODocument("classIndexManagerTestClass");
    docOne.field("prop0", "doc1-prop0");
    docOne.field("prop1", "doc1-prop1");
    docOne.save();

    final ODocument docTwo = new ODocument("classIndexManagerTestClass");
    docTwo.field("prop0", "doc2-prop0");
    docTwo.field("prop1", "doc2-prop1");
    docTwo.save();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.getClass("classIndexManagerTestClass");
    final OIndex<?> oIndex = oClass.getClassIndex("classIndexManagerTestIndexOnPropertiesFromClassAndSuperclass");

    Assert.assertEquals(oIndex.getSize(), 2);
  }
}
