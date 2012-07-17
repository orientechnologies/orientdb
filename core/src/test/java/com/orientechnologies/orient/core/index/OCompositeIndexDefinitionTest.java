package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
@SuppressWarnings("unchecked")
public class OCompositeIndexDefinitionTest {
  private OCompositeIndexDefinition compositeIndex;

  @BeforeMethod
  public void beforeMethod() {
    compositeIndex = new OCompositeIndexDefinition("testClass");

    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fOne", OType.INTEGER));
    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fTwo", OType.STRING));
  }

  @Test
  public void testGetFields() {
    final List<String> fields = compositeIndex.getFields();

    Assert.assertEquals(fields.size(), 2);
    Assert.assertEquals(fields.get(0), "fOne");
    Assert.assertEquals(fields.get(1), "fTwo");
  }

  @Test
  public void testCreateValueSuccessful() {
    final Object result = compositeIndex.createValue(Arrays.asList("12", "test"));

    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateMapValueSuccessful() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyMapIndexDefinition("testCollectionClass", "fTwo", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    final Object result = compositeIndexDefinition.createValue(12, stringMap);

    final Collection<OCompositeKey> collectionResult = (Collection<OCompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key2")));
  }

  @Test
  public void testCreateCollectionValueSuccessfulOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(12, Arrays.asList(1, 2));

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1));
    expectedResult.add(new OCompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));

    final Object result = compositeIndexDefinition.createValue(Arrays.asList(Arrays.asList(1, 2), 12));

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(1, 12));
    expectedResult.add(new OCompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testCreateCollectionValueSuccessfulThree() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.STRING));

    final Object result = compositeIndexDefinition.createValue(12, Arrays.asList(1, 2), "test");

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1, "test"));
    expectedResult.add(new OCompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateCollectionValueTwoCollections() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));

    compositeIndexDefinition.createValue(Arrays.asList(1, 2), Arrays.asList(12));
  }

  @Test
  public void testCreateValueWrongParam() {
    final Object result = compositeIndex.createValue(Arrays.asList("1t2", "test"));
    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueSuccessfulArrayParams() {
    final Object result = compositeIndex.createValue("12", "test");

    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateValueWrongParamArrayParams() {
    final Object result = compositeIndex.createValue("1t2", "test");
    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueDefinitionsMoreThanParams() {
    compositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fThree", OType.STRING));

    final Object result = compositeIndex.createValue("12", "test");
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testCreateValueIndexItemWithTwoParams() {
    final OCompositeIndexDefinition anotherCompositeIndex = new OCompositeIndexDefinition("testClass");

    anotherCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "f11", OType.STRING));
    anotherCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "f22", OType.STRING));

    compositeIndex.addIndex(anotherCompositeIndex);

    final Object result = compositeIndex.createValue("12", "test", "tset");
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test", "tset")));
  }

  @Test
  public void testDocumentToIndexSuccessful() {
    final ODocument document = new ODocument();

    document.field("fOne", 12);
    document.field("fTwo", "test");

    final Object result = compositeIndex.getDocumentValueToIndex(document);
    Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
  }

  @Test
  public void testDocumentToIndexMapValueSuccessful() {
    final ODocument document = new ODocument();

    final Map<String, String> stringMap = new HashMap<String, String>();
    stringMap.put("key1", "val1");
    stringMap.put("key2", "val2");

    document.field("fOne", 12);
    document.field("fTwo", stringMap);

    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyMapIndexDefinition("testCollectionClass", "fTwo", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(document);
    final Collection<OCompositeKey> collectionResult = (Collection<OCompositeKey>) result;

    Assert.assertEquals(collectionResult.size(), 2);
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key1")));
    Assert.assertTrue(collectionResult.contains(new OCompositeKey(12, "key2")));
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulOne() {
    final ODocument document = new ODocument();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1));
    expectedResult.add(new OCompositeKey(12, 2));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulTwo() {
    final ODocument document = new ODocument();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(1, 12));
    expectedResult.add(new OCompositeKey(2, 12));

    Assert.assertEquals(result, expectedResult);
  }

  @Test
  public void testDocumentToIndexCollectionValueSuccessfulThree() {
    final ODocument document = new ODocument();

    document.field("fOne", 12);
    document.field("fTwo", Arrays.asList(1, 2));
    document.field("fThree", "test");

    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.STRING));

    final Object result = compositeIndexDefinition.getDocumentValueToIndex(document);

    final ArrayList<OCompositeKey> expectedResult = new ArrayList<OCompositeKey>();

    expectedResult.add(new OCompositeKey(12, 1, "test"));
    expectedResult.add(new OCompositeKey(12, 2, "test"));

    Assert.assertEquals(result, expectedResult);
  }

  @Test(expectedExceptions = OException.class)
  public void testDocumentToIndexCollectionValueTwoCollections() {
    final ODocument document = new ODocument();

    document.field("fOne", Arrays.asList(12));
    document.field("fTwo", Arrays.asList(1, 2));

    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition("testCollectionClass");

    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.INTEGER));
    compositeIndexDefinition.getDocumentValueToIndex(document);
  }

  @Test
  public void testDocumentToIndexWrongField() {
    final ODocument document = new ODocument();

    document.field("fOne", "1t2");
    document.field("fTwo", "test");

    final Object result = compositeIndex.getDocumentValueToIndex(document);
    Assert.assertNull(result);
  }

  @Test
  public void testGetParamCount() {
    final int result = compositeIndex.getParamCount();

    Assert.assertEquals(result, 2);
  }

  @Test
  public void testGetTypes() {
    final OType[] result = compositeIndex.getTypes();

    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], OType.INTEGER);
    Assert.assertEquals(result[1], OType.STRING);
  }

  @Test
  public void testEmptyIndexReload() {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx("memory:compositetestone");
    database.create();

    final OCompositeIndexDefinition emptyCompositeIndex = new OCompositeIndexDefinition("testClass");

    emptyCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fOne", OType.INTEGER));
    emptyCompositeIndex.addIndex(new OPropertyIndexDefinition("testClass", "fTwo", OType.STRING));

    final ODocument docToStore = emptyCompositeIndex.toStream();
    database.save(docToStore);

    final ODocument docToLoad = database.load(docToStore.getIdentity());

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndex);
  }

  @Test
  public void testIndexReload() {
    final ODocument docToStore = compositeIndex.toStream();

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToStore);

    Assert.assertEquals(result, compositeIndex);
  }

  @Test
  public void testClassOnlyConstructor() {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx("memory:compositetesttwo");
    database.create();

    final OCompositeIndexDefinition emptyCompositeIndex = new OCompositeIndexDefinition("testClass", Arrays.asList(
        new OPropertyIndexDefinition("testClass", "fOne", OType.INTEGER), new OPropertyIndexDefinition("testClass", "fTwo",
            OType.STRING)));

    final OCompositeIndexDefinition emptyCompositeIndexTwo = new OCompositeIndexDefinition("testClass");

    emptyCompositeIndexTwo.addIndex(new OPropertyIndexDefinition("testClass", "fOne", OType.INTEGER));
    emptyCompositeIndexTwo.addIndex(new OPropertyIndexDefinition("testClass", "fTwo", OType.STRING));

    Assert.assertEquals(emptyCompositeIndex, emptyCompositeIndexTwo);

    final ODocument docToStore = emptyCompositeIndex.toStream();
    database.save(docToStore);

    final ODocument docToLoad = database.load(docToStore.getIdentity());

    final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, emptyCompositeIndexTwo);
  }

  public void testProcessChangeListEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.STRING));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();

    trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
        firedEvents.add(event);
      }
    });

    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<Integer, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l3", 3)));
  }

  public void testProcessChangeListEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.STRING));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedList<String> trackedList = new OTrackedList<String>(doc);
    final List<OMultiValueChangeEvent<Integer, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Integer, String>>();

    trackedList.add("l1");
    trackedList.add("l2");
    trackedList.add("l3");
    trackedList.remove("l2");

    trackedList.addChangeListener(new OMultiValueChangeListener<Integer, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<Integer, String> event) {
        firedEvents.add(event);
      }
    });

    trackedList.add("l4");
    trackedList.remove("l1");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<Integer, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "l1", 3)));
  }

  public void testProcessChangeSetEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.STRING));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedSet = new OTrackedSet<String>(doc);
    final List<OMultiValueChangeEvent<String, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<String, String>>();

    trackedSet.addChangeListener(new OMultiValueChangeListener<String, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
        firedEvents.add(event);
      }
    });

    trackedSet.add("l1");
    trackedSet.add("l2");
    trackedSet.add("l3");
    trackedSet.remove("l2");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<String, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l3", 3)));
  }

  public void testProcessChangeSetEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyListIndexDefinition("testCollectionClass", "fTwo", OType.STRING));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedSet<String> trackedMap = new OTrackedSet<String>(doc);
    final List<OMultiValueChangeEvent<String, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<String, String>>();

    trackedMap.add("l1");
    trackedMap.add("l2");
    trackedMap.add("l3");
    trackedMap.remove("l2");

    trackedMap.addChangeListener(new OMultiValueChangeListener<String, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<String, String> event) {
        firedEvents.add(event);
      }
    });

    trackedMap.add("l4");
    trackedMap.remove("l1");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<String, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "l4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "l1", 3)));
  }

  public void testProcessChangeKeyMapEventsOne() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyMapIndexDefinition("testCollectionClass", "fTwo", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);
    final List<OMultiValueChangeEvent<Object, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Object, String>>();

    trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
        firedEvents.add(event);
      }
    });

    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<Object, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 0);
    Assert.assertEquals(keysToAdd.size(), 2);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k1", 3)));
    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k3", 3)));
  }

  public void testProcessChangeKeyMapEventsTwo() {
    final OCompositeIndexDefinition compositeIndexDefinition = new OCompositeIndexDefinition();

    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fOne", OType.INTEGER));
    compositeIndexDefinition.addIndex(new OPropertyMapIndexDefinition("testCollectionClass", "fTwo", OType.STRING,
        OPropertyMapIndexDefinition.INDEX_BY.KEY));
    compositeIndexDefinition.addIndex(new OPropertyIndexDefinition("testCollectionClass", "fThree", OType.INTEGER));

    final ODocument doc = new ODocument();
    doc.unsetDirty();
    Assert.assertFalse(doc.isDirty());

    final OTrackedMap<String> trackedMap = new OTrackedMap<String>(doc);

    trackedMap.put("k1", "v1");
    trackedMap.put("k2", "v2");
    trackedMap.put("k3", "v3");
    trackedMap.remove("k2");

    final List<OMultiValueChangeEvent<Object, String>> firedEvents = new ArrayList<OMultiValueChangeEvent<Object, String>>();

    trackedMap.addChangeListener(new OMultiValueChangeListener<Object, String>() {
      public void onAfterRecordChanged(final OMultiValueChangeEvent<Object, String> event) {
        firedEvents.add(event);
      }
    });

    trackedMap.put("k4", "v4");
    trackedMap.remove("k1");

    Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
    Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

    for (OMultiValueChangeEvent<Object, String> multiValueChangeEvent : firedEvents)
      compositeIndexDefinition.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove, 2, 3);

    Assert.assertEquals(keysToRemove.size(), 1);
    Assert.assertEquals(keysToAdd.size(), 1);

    Assert.assertTrue(keysToAdd.containsKey(new OCompositeKey(2, "k4", 3)));
    Assert.assertTrue(keysToRemove.containsKey(new OCompositeKey(2, "k1", 3)));
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", compositeIndex.getClassName());
  }
}
