package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author LomakiA <a href="mailto:a.lomakin@orientechnologies.com">Andrey Lomakin</a>
 * @since 20.12.11
 */
@Test
public class OPropertyListIndexDefinitionTest {
  private OPropertyListIndexDefinition propertyIndex;

  @BeforeMethod
  public void beforeMethod() {
    propertyIndex = new OPropertyListIndexDefinition("testClass", "fOne", OType.INTEGER);
  }

  public void testCreateValueSingleParameter() {
    final Object result = propertyIndex.createValue(Collections.singletonList(Arrays.asList("12", "23")));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  public void testCreateValueTwoParameters() {
    final Object result = propertyIndex.createValue(Arrays.asList(Arrays.asList("12", "23"), "25"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  public void testCreateValueWrongParameter() {
    try {
      propertyIndex.createValue(Collections.singletonList("tt"));
      Assert.fail();
    } catch (OIndexException x) {

    }

  }

  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue((Object) Arrays.asList("12", "23"));

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue(Arrays.asList("12", "23"), "25");

    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  public void testCreateValueWrongParameterArrayParams() {
    Assert.assertNull(propertyIndex.createValue("tt"));
  }

  public void testGetDocumentValueToIndex() {
    final ODocument document = new ODocument();

    document.field("fOne", Arrays.asList("12", "23"));
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(document);
    Assert.assertTrue(result instanceof Collection);

    final Collection<?> collectionResult = (Collection<?>) result;
    Assert.assertEquals(collectionResult.size(), 2);

    Assert.assertTrue(collectionResult.contains(12));
    Assert.assertTrue(collectionResult.contains(23));
  }

  public void testCreateSingleValue() {
    final Object result = propertyIndex.createSingleValue("12");
    Assert.assertEquals(result, 12);
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateSingleValueWrongParameter() {
    propertyIndex.createSingleValue("tt");
  }

  public void testProcessChangeEventAddOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddOnceWithConversion() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, String> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, String>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, "42");
    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddTwoTimes() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 1, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 2);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddTwoValues() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 1, 43);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    addedKeys.put(43, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventRemoveOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventRemoveOnceWithConversion() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, String> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, String>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, "42");

    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventRemoveTwoTimes() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 2);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddTwoTimesInvValue() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 1, 555);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    addedKeys.put(555, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddRemove() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddRemoveInvValue() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 55);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);
    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(55, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddTwiceRemoveOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 1, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(42, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventAddOnceRemoveTwice() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventRemoveTwoTimesAddOnce() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventOne = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventTwo = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, 42);
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEventThree = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.ADD, 1, 42);

    propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);
    propertyIndex.processChangeEvent(multiValueChangeEventThree, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

  public void testProcessChangeEventUpdate() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, Integer> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, Integer>(
        OMultiValueChangeEvent.OChangeType.UPDATE, 0, 41, 42);

    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(41, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);

  }

  public void testProcessChangeEventUpdateConvertValues() {
    final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
    final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
    final OMultiValueChangeEvent<Integer, String> multiValueChangeEvent = new OMultiValueChangeEvent<Integer, String>(
        OMultiValueChangeEvent.OChangeType.UPDATE, 0, "41", "42");

    propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

    final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
    addedKeys.put(41, 1);

    final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
    removedKeys.put(42, 1);

    Assert.assertEquals(keysToAdd, addedKeys);
    Assert.assertEquals(keysToRemove, removedKeys);
  }

}
