package com.orientechnologies.orient.core.index;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class OCompositeIndexDefinitionTest {
	private OCompositeIndexDefinition	compositeIndex;

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
		final Comparable<?> result = compositeIndex.createValue(Arrays.asList("12", "test"));

		Assert.assertEquals(result, new OCompositeKey(Arrays.asList(12, "test")));
	}

	@Test
	public void testCreateValueWrongParam() {
		final Comparable<?> result = compositeIndex.createValue(Arrays.asList("1t2", "test"));
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
		docToStore.save();

		final ODocument docToLoad = database.load(docToStore.getIdentity());

		final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
		result.fromStream(docToLoad);

		database.delete();
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
		docToStore.save();

		final ODocument docToLoad = database.load(docToStore.getIdentity());

		final OCompositeIndexDefinition result = new OCompositeIndexDefinition();
		result.fromStream(docToLoad);

		database.delete();
		Assert.assertEquals(result, emptyCompositeIndexTwo);
	}

	@Test
	public void testClassName() {
		Assert.assertEquals("testClass", compositeIndex.getClassName());
	}
}
