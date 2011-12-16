package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Collection;

@Test(groups = { "index" })
public class PropertyIndexTest {
	private final ODatabaseDocumentTx	database;

	@Parameters(value = "url")
	public PropertyIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeClass
	public void beforeClass() {
		if (database.isClosed())
			database.open("admin", "admin");

		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.createClass("PropertyIndexTestClass");
		oClass.createProperty("prop1", OType.STRING);
		oClass.createProperty("prop2", OType.INTEGER);
		oClass.createProperty("prop3", OType.BOOLEAN);
		oClass.createProperty("prop4", OType.INTEGER);
		oClass.createProperty("prop5", OType.STRING);

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
		database.close();
	}

	@AfterClass
	public void afterClass() {
		if (database.isClosed())
			database.open("admin", "admin");
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

		propOne.createIndex(OClass.INDEX_TYPE.UNIQUE);

		final Collection<OIndex<?>> indexes = propOne.getIndexes();
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

		schema.save();
	}

	@Test(dependsOnMethods = { "testCreateUniqueIndex" })
	public void createAdditionalSchemas() {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");

		oClass.createIndex("propOne1", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");
		oClass.createIndex("propOne2", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop3");
		oClass.createIndex("propOne3", OClass.INDEX_TYPE.UNIQUE, "prop2", "prop3");
		oClass.createIndex("propOne4", OClass.INDEX_TYPE.UNIQUE, "prop2", "prop1");

		schema.save();
	}

	@Test(dependsOnMethods = "createAdditionalSchemas")
	public void testGetIndexes() {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");
		final OProperty propOne = oClass.getProperty("prop1");

		final Collection<OIndex<?>> indexes = propOne.getIndexes();
		Assert.assertEquals(indexes.size(), 3);
		Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
		Assert.assertNotNull(containsIndex(indexes, "propOne1"));
		Assert.assertNotNull(containsIndex(indexes, "propOne2"));
	}

	@Test(dependsOnMethods = "createAdditionalSchemas")
	public void testGetAllIndexes() {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");
		final OProperty propOne = oClass.getProperty("prop1");

		final Collection<OIndex<?>> indexes = propOne.getAllIndexes();
		Assert.assertEquals(indexes.size(), 4);
		Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
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

	@Test(dependsOnMethods = { "testCreateUniqueIndex" })
	public void testIsIndexedIndexedField() {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");
		final OProperty propOne = oClass.getProperty("prop1");
		Assert.assertTrue(propOne.isIndexed());
	}

	@Test
	public void testDropIndexes() throws Exception {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");

		oClass.createIndex("PropertyIndexFirstIndex", OClass.INDEX_TYPE.UNIQUE, "prop4");
		oClass.createIndex("PropertyIndexSecondIndex", OClass.INDEX_TYPE.UNIQUE, "pROp4");

		oClass.getProperty("prop4").dropIndexes();

		Assert.assertNull(database.getMetadata().getIndexManager().getIndex("PropertyIndexFirstIndex"));
		Assert.assertNull(database.getMetadata().getIndexManager().getIndex("PropertyIndexSecondIndex"));
	}

	@Test
	public void testDropIndexesForComposite() throws Exception {
		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.getClass("PropertyIndexTestClass");

		oClass.createIndex("PropertyIndexFirstIndex", OClass.INDEX_TYPE.UNIQUE, "pROp4");
		oClass.createIndex("PropertyIndexSecondIndex", OClass.INDEX_TYPE.UNIQUE, "prop4", "prop5");

		try {
			oClass.getProperty("prop4").dropIndexes();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			Assert.assertEquals(e.getMessage(), "This operation applicable only for property indexes. "
					+ "PropertyIndexSecondIndex is OCompositeIndexDefinition{indexDefinitions=["
					+ "OPropertyIndexDefinition{className='PropertyIndexTestClass', field='prop4', keyType=INTEGER}, "
					+ "OPropertyIndexDefinition{className='PropertyIndexTestClass', field='prop5', keyType=STRING}], "
					+ "className='PropertyIndexTestClass'}");
		}

		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("PropertyIndexFirstIndex"));
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("PropertyIndexSecondIndex"));
	}

	private OIndex containsIndex(final Collection<OIndex<?>> indexes, final String indexName) {
		for (final OIndex index : indexes) {
			if (index.getName().equals(indexName))
				return index;
		}
		return null;
	}
}
