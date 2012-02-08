package com.orientechnologies.orient.core.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
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

		final Collection collectionResult = (Collection) result;
		Assert.assertEquals(collectionResult.size(), 2);

		Assert.assertTrue(collectionResult.contains(12));
		Assert.assertTrue(collectionResult.contains(23));
	}

	public void testCreateValueTwoParameters() {
		final Object result = propertyIndex.createValue(Arrays.asList(Arrays.asList("12", "23"), "25"));

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;
		Assert.assertEquals(collectionResult.size(), 2);

		Assert.assertTrue(collectionResult.contains(12));
		Assert.assertTrue(collectionResult.contains(23));
	}

	public void testCreateValueWrongParameter() {
		final Object result = propertyIndex.createValue(Collections.singletonList("tt"));
		Assert.assertNull(result);
	}

	public void testCreateValueSingleParameterArrayParams() {
		final Object result = propertyIndex.createValue((Object) Arrays.asList("12", "23"));

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;
		Assert.assertEquals(collectionResult.size(), 2);

		Assert.assertTrue(collectionResult.contains(12));
		Assert.assertTrue(collectionResult.contains(23));
	}

	public void testCreateValueTwoParametersArrayParams() {
		final Object result = propertyIndex.createValue(Arrays.asList("12", "23"), "25");

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;
		Assert.assertEquals(collectionResult.size(), 2);

		Assert.assertTrue(collectionResult.contains(12));
		Assert.assertTrue(collectionResult.contains(23));
	}

	public void testCreateValueWrongParameterArrayParams() {
		final Object result = propertyIndex.createValue("tt");
		Assert.assertNull(result);
	}

	public void testGetDocumentValueToIndex() {
		final ODocument document = new ODocument();

		document.field("fOne", Arrays.asList("12", "23"));
		document.field("fTwo", 10);

		final Object result = propertyIndex.getDocumentValueToIndex(document);
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;
		Assert.assertEquals(collectionResult.size(), 2);

		Assert.assertTrue(collectionResult.contains(12));
		Assert.assertTrue(collectionResult.contains(23));
	}

	public void testCreateSingleValue() {
		final Object result = propertyIndex.createSingleValue("12");
		Assert.assertEquals(result, 12);
	}

	public void testCreateSingleValueWrongParameter() {
		final Object result = propertyIndex.createSingleValue("tt");
		Assert.assertNull(result);
	}

	public void testProcessChangeEventAddOnce() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
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
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, "42");
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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, 42);

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, 43);

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
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

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
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, "42");

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, 42);

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, "ttry");

		propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
		propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(42, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();


		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventAddRemove() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, "ttv");

		propertyIndex.processChangeEvent(multiValueChangeEventOne, keysToAdd, keysToRemove);
		propertyIndex.processChangeEvent(multiValueChangeEventTwo, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(42, 1);
		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventAddTwiceRemoveOnce() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, 42);
		final OMultiValueChangeEvent multiValueChangeEventThree = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 0, 42);
		final OMultiValueChangeEvent multiValueChangeEventThree = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);

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
		final OMultiValueChangeEvent multiValueChangeEventOne = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 0, null, 42);
		final OMultiValueChangeEvent multiValueChangeEventTwo = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, 1, null, 42);
		final OMultiValueChangeEvent multiValueChangeEventThree = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, 1, 42);


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
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.UPDATE, 0, 41, 42);

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
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.UPDATE, 0, "41", "42");

		propertyIndex.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(41, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(42, 1);

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

}
