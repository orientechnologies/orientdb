package com.orientechnologies.orient.core.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 20.12.11
 */
@Test
public class OPropertyMapIndexDefinitionTest {
	private OPropertyMapIndexDefinition propertyIndexByKey;
	private OPropertyMapIndexDefinition propertyIndexByValue;
	private OPropertyMapIndexDefinition propertyIndexByIntegerKey;

	private final Map<String, Integer> mapToTest = new HashMap<String, Integer>();

	@BeforeClass
	public void beforeClass() {
		mapToTest.put("st1", 1);
		mapToTest.put("st2", 2);
	}

	@BeforeMethod
	public void beforeMethod() {
		propertyIndexByKey = new OPropertyMapIndexDefinition("testClass", "fOne", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);
		propertyIndexByIntegerKey = new OPropertyMapIndexDefinition("testClass", "fTwo", OType.INTEGER, OPropertyMapIndexDefinition.INDEX_BY.KEY);
		propertyIndexByValue = new OPropertyMapIndexDefinition("testClass", "fOne", OType.INTEGER, OPropertyMapIndexDefinition.INDEX_BY.VALUE);
	}

	public void testCreateValueByKeySingleParameter() {
		final Object result = propertyIndexByKey.createValue(Collections.singletonList(mapToTest));
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains("st1"));
		Assert.assertTrue(collectionResult.contains("st2"));
	}

	public void testCreateValueByValueSingleParameter() {
		final Object result = propertyIndexByValue.createValue(Collections.singletonList(mapToTest));
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains(1));
		Assert.assertTrue(collectionResult.contains(2));
	}

	public void testCreateValueByKeyTwoParameters() {
		final Object result = propertyIndexByKey.createValue(Arrays.asList(mapToTest, "25"));

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains("st1"));
		Assert.assertTrue(collectionResult.contains("st2"));
	}

	public void testCreateValueByValueTwoParameters() {
		final Object result = propertyIndexByValue.createValue(Arrays.asList(mapToTest, "25"));

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains(1));
		Assert.assertTrue(collectionResult.contains(2));
	}

	public void testCreateValueWrongParameter() {
		final Object result = propertyIndexByKey.createValue(Collections.singletonList("tt"));
		Assert.assertNull(result);
	}

	public void testCreateValueByKeySingleParameterArrayParams() {
		final Object result = propertyIndexByKey.createValue(mapToTest);
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains("st1"));
		Assert.assertTrue(collectionResult.contains("st2"));
	}

	public void testCreateValueByValueSingleParameterArrayParams() {
		final Object result = propertyIndexByValue.createValue(mapToTest);
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains(1));
		Assert.assertTrue(collectionResult.contains(2));
	}

	public void testCreateValueByKeyTwoParametersArrayParams() {
		final Object result = propertyIndexByKey.createValue(mapToTest, "25");

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains("st1"));
		Assert.assertTrue(collectionResult.contains("st2"));
	}

	public void testCreateValueByValueTwoParametersArrayParams() {
		final Object result = propertyIndexByValue.createValue(mapToTest, "25");

		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains(1));
		Assert.assertTrue(collectionResult.contains(2));
	}


	public void testCreateValueWrongParameterArrayParams() {
		final Object result = propertyIndexByKey.createValue("tt");
		Assert.assertNull(result);
	}

	public void testGetDocumentValueByKeyToIndex() {
		final ODocument document = new ODocument();

		document.field("fOne", mapToTest);
		document.field("fTwo", 10);

		final Object result = propertyIndexByKey.getDocumentValueToIndex(document);
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains("st1"));
		Assert.assertTrue(collectionResult.contains("st2"));
	}

	public void testGetDocumentValueByValueToIndex() {
		final ODocument document = new ODocument();

		document.field("fOne", mapToTest);
		document.field("fTwo", 10);

		final Object result = propertyIndexByValue.getDocumentValueToIndex(document);
		Assert.assertTrue(result instanceof Collection);

		final Collection collectionResult = (Collection) result;

		Assert.assertEquals(collectionResult.size(), 2);
		Assert.assertTrue(collectionResult.contains(1));
		Assert.assertTrue(collectionResult.contains(2));
	}

	public void testGetFields() {
		final List<String> result = propertyIndexByKey.getFields();
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(result.get(0), "fOne");
	}

	public void testGetTypes() {
		final OType[] result = propertyIndexByKey.getTypes();
		Assert.assertEquals(result.length, 1);
		Assert.assertEquals(result[0], OType.STRING);
	}

	public void testEmptyIndexByKeyReload() {
		final ODatabaseDocumentTx database = new ODatabaseDocumentTx("memory:propertytest");
		database.create();

		propertyIndexByKey = new OPropertyMapIndexDefinition("tesClass", "fOne", OType.STRING, OPropertyMapIndexDefinition.INDEX_BY.KEY);

		final ODocument docToStore = propertyIndexByKey.toStream();
		database.save(docToStore);

		final ODocument docToLoad = database.load(docToStore.getIdentity());

		final OPropertyIndexDefinition result = new OPropertyMapIndexDefinition();
		result.fromStream(docToLoad);

		database.drop();
		Assert.assertEquals(result, propertyIndexByKey);
	}

	public void testEmptyIndexByValueReload() {
		final ODatabaseDocumentTx database = new ODatabaseDocumentTx("memory:propertytest");
		database.create();

		propertyIndexByValue = new OPropertyMapIndexDefinition("tesClass", "fOne", OType.INTEGER, OPropertyMapIndexDefinition.INDEX_BY.VALUE);

		final ODocument docToStore = propertyIndexByValue.toStream();
		database.save(docToStore);

		final ODocument docToLoad = database.load(docToStore.getIdentity());

		final OPropertyIndexDefinition result = new OPropertyMapIndexDefinition();
		result.fromStream(docToLoad);

		database.drop();
		Assert.assertEquals(result, propertyIndexByValue);
	}

	public void testCreateSingleValueByKey() {
		final Object result = propertyIndexByKey.createSingleValue("tt");
		Assert.assertEquals(result, "tt");
	}

	public void testCreateSingleValueByValue() {
		final Object result = propertyIndexByValue.createSingleValue("12");
		Assert.assertEquals(result, 12);
	}

	public void testCreateWrongSingleValueByValue() {
		final Object result = propertyIndexByValue.createSingleValue("tt");
		Assert.assertNull(result);
	}


	@Test(expectedExceptions = NullPointerException.class)
	public void testIndexByIsRequired() {
		new OPropertyMapIndexDefinition("testClass", "testField", OType.STRING, null);
	}

	public void testCreateDDLByKey() {
		final String ddl = propertyIndexByKey.toCreateIndexDDL("testIndex", "unique").toLowerCase();
		Assert.assertEquals(ddl, "create index testindex on testclass ( fone by key ) unique");
	}

	public void testCreateDDLByValue() {
		final String ddl = propertyIndexByValue.toCreateIndexDDL("testIndex", "unique").toLowerCase();
		Assert.assertEquals(ddl, "create index testindex on testclass ( fone by value ) unique");
	}

	public void testProcessChangeEventAddKey() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key1", "value1");

		propertyIndexByKey.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put("key1", 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventAddKeyWithConversion() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "12", "value1");

		propertyIndexByIntegerKey.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(12, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventAddValue() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "key1", 42);

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(42, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventAddValueWithConversion() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.ADD, "12", "42");

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(42, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventRemoveKey() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, "key1", "value1");

		propertyIndexByKey.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put("key1", 1);

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventRemoveKeyWithConversion() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, "12", "value1");

		propertyIndexByIntegerKey.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(12, 1);

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventRemoveValue() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, "key1", null, 42);

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(42, 1);

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventRemoveValueWithConversion() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.REMOVE, "12", null, "42");

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(42, 1);


		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventUpdateKey() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.UPDATE, "key1", 42);

		propertyIndexByKey.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);
		Assert.assertTrue(keysToAdd.isEmpty());
		Assert.assertTrue(keysToRemove.isEmpty());
	}

	public void testProcessChangeEventUpdateValue() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.UPDATE, "key1", 41, 42);

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(41, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(42, 1);


		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}

	public void testProcessChangeEventUpdateValueWithConversion() {
		final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
		final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();
		final OMultiValueChangeEvent multiValueChangeEvent = new OMultiValueChangeEvent(OMultiValueChangeEvent.OChangeType.UPDATE, "12", "42", "41");

		propertyIndexByValue.processChangeEvent(multiValueChangeEvent, keysToAdd, keysToRemove);

		final Map<Object, Integer> addedKeys = new HashMap<Object, Integer>();
		addedKeys.put(42, 1);

		final Map<Object, Integer> removedKeys = new HashMap<Object, Integer>();
		removedKeys.put(41, 1);

		Assert.assertEquals(keysToAdd, addedKeys);
		Assert.assertEquals(keysToRemove, removedKeys);
	}


}
