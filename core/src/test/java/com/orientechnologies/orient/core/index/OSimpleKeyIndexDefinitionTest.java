package com.orientechnologies.orient.core.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class OSimpleKeyIndexDefinitionTest {

	private OSimpleKeyIndexDefinition	simpleKeyIndexDefinition;

	@BeforeMethod
	public void beforeMethod() {
		simpleKeyIndexDefinition = new OSimpleKeyIndexDefinition(OType.INTEGER, OType.STRING);
	}

	@Test
	public void testGetFields() {
		Assert.assertTrue(simpleKeyIndexDefinition.getFields().isEmpty());
	}

	@Test
	public void testGetClassName() {
		Assert.assertNull(simpleKeyIndexDefinition.getClassName());
	}

	@Test
	public void testCreateValueSimpleKey() {
		final OSimpleKeyIndexDefinition keyIndexDefinition = new OSimpleKeyIndexDefinition(OType.INTEGER);
		final Object result = keyIndexDefinition.createValue("2");
		Assert.assertEquals(result, 2);
	}

	@Test
	public void testCreateValueCompositeKeyListParam() {
		final Object result = simpleKeyIndexDefinition.createValue(Arrays.asList("2", "3"));

		final OCompositeKey compositeKey = new OCompositeKey(Arrays.asList(2, "3"));
		Assert.assertEquals(result, compositeKey);
	}

	@Test
	public void testCreateValueCompositeKeyNullListParam() {
		final Object result = simpleKeyIndexDefinition.createValue(Arrays.asList((Object) null));

		Assert.assertNull(result);
	}

	@Test
	public void testNullParamListItem() {
		final Object result = simpleKeyIndexDefinition.createValue(Arrays.asList("2", null));

		Assert.assertNull(result);
	}

	@Test
	public void testWrongParamTypeListItem() {
		final Object result = simpleKeyIndexDefinition.createValue(Arrays.asList("a", "3"));

		Assert.assertNull(result);
	}

	@Test
	public void testCreateValueCompositeKey() {
		final Object result = simpleKeyIndexDefinition.createValue("2", "3");

		final OCompositeKey compositeKey = new OCompositeKey(Arrays.asList(2, "3"));
		Assert.assertEquals(result, compositeKey);
	}

	@Test
	public void testCreateValueCompositeKeyNullParamList() {
		final Object result = simpleKeyIndexDefinition.createValue((List) null);

		Assert.assertNull(result);
	}

	@Test
	public void testCreateValueCompositeKeyNullParam() {
		final Object result = simpleKeyIndexDefinition.createValue((Object) null);

		Assert.assertNull(result);
	}

	@Test
	public void testCreateValueCompositeKeyEmptyList() {
		final Object result = simpleKeyIndexDefinition.createValue(Collections.<Object> emptyList());

		Assert.assertNull(result);
	}

	@Test
	public void testNullParamItem() {
		final Object result = simpleKeyIndexDefinition.createValue("2", null);

		Assert.assertNull(result);
	}

	@Test
	public void testWrongParamType() {
		final Object result = simpleKeyIndexDefinition.createValue("a", "3");

		Assert.assertNull(result);
	}

	@Test
	public void testParamCount() {
		Assert.assertEquals(simpleKeyIndexDefinition.getParamCount(), 2);
	}

	@Test
	public void testParamCountOneItem() {
		final OSimpleKeyIndexDefinition keyIndexDefinition = new OSimpleKeyIndexDefinition(OType.INTEGER);

		Assert.assertEquals(keyIndexDefinition.getParamCount(), 1);
	}

	@Test
	public void testGetKeyTypes() {
		Assert.assertEquals(simpleKeyIndexDefinition.getTypes(), new OType[] { OType.INTEGER, OType.STRING });
	}

	@Test
	public void testGetKeyTypesOneType() {
		final OSimpleKeyIndexDefinition keyIndexDefinition = new OSimpleKeyIndexDefinition(OType.BOOLEAN);

		Assert.assertEquals(keyIndexDefinition.getTypes(), new OType[] { OType.BOOLEAN });
	}

	@Test
	public void testReload() {
		final ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("memory:osimplekeyindexdefinitiontest");
		databaseDocumentTx.create();

		final ODocument storeDocument = simpleKeyIndexDefinition.toStream();
		storeDocument.save();

		final ODocument loadDocument = databaseDocumentTx.load(storeDocument.getIdentity());
		final OSimpleKeyIndexDefinition loadedKeyIndexDefinition = new OSimpleKeyIndexDefinition();
		loadedKeyIndexDefinition.fromStream(loadDocument);

		databaseDocumentTx.close();
		databaseDocumentTx.delete();

		Assert.assertEquals(loadedKeyIndexDefinition, simpleKeyIndexDefinition);
	}

	@Test(expectedExceptions = OIndexException.class)
	public void testGetDocumentValueToIndex() {
		simpleKeyIndexDefinition.getDocumentValueToIndex(new ODocument());
	}

}
