package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public abstract class OPropertyRidBagAbstractIndexDefinition {
  private OPropertyRidBagIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new OPropertyRidBagIndexDefinition("testClass", "fOne");
  }

  @Test
  public void testCreateValueSingleParameter() {
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(Collections.singletonList(ridBag));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));
  }

  @Test
  public void testCreateValueTwoParameters() {
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(Arrays.asList(ridBag, "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));
  }

  @Test
  public void testCreateValueWrongParameter() {
    Assert.assertNull(propertyIndex.createValue(Collections.singletonList("tt")));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue((Object) ridBag);

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final Object result = propertyIndex.createValue(ridBag, "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    Assert.assertNull(propertyIndex.createValue("tt"));
  }

  @Test
  public void testGetDocumentValueToIndex() {
    ORidBag ridBag = new ORidBag();
    ridBag.setAutoConvertToRecord(false);

    ridBag.add(new ORecordId("#1:12"));
    ridBag.add(new ORecordId("#1:23"));

    final ODocument document = new ODocument();

    document.field("fOne", ridBag);
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:12")));
    Assert.assertTrue(collectionResult.contains(new ORecordId("#1:23")));
  }

  @Test
  public void testProcessChangeEventAddOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();

    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEvent =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwoTimes() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 2);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwoValues() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:13"), new ORecordId("#1:13"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);
    addedKeys.put(new ORecordId("#1:13"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEvent =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimes() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 2);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemove() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddRemoveInvValue() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:13"),
            null,
            new ORecordId("#1:13"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:13"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddTwiceRemoveOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(new ORecordId("#1:12"), 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventAddOnceRemoveTwice() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  @Test
  public void testProcessChangeEventRemoveTwoTimesAddOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventOne =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventTwo =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE,
            new ORecordId("#1:12"),
            null,
            new ORecordId("#1:12"));
    final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> multiValueChangeEventThree =
        new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.ADD, new ORecordId("#1:12"), new ORecordId("#1:12"));

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(new ORecordId("#1:12"), 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }
}
