package com.orientechnologies.orient.test.database.auto;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = { "index" })
public class SQLDropPropertyIndexTest {
	private final ODatabaseDocumentTx	database;
	private static final OType				EXPECTED_PROP1_TYPE	= OType.DOUBLE;
	private static final OType				EXPECTED_PROP2_TYPE	= OType.INTEGER;

	@Parameters(value = "url")
	public SQLDropPropertyIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");

		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.createClass("DropPropertyIndexTestClass");
		oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
		oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);

		schema.save();
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		database.command(new OCommandSQL("drop class DropPropertyIndexTestClass")).execute();
		database.getMetadata().getSchema().reload();
		database.close();
	}

	@Test
	public void testForcePropertyEnabled() throws Exception {
		database.command(
				new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2, prop1) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		OIndex index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");
		Assert.assertNotNull(index);

		database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndexTestClass.prop1 FORCE")).execute();
		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");

		Assert.assertNull(index);
	}

	@Test
	public void testForcePropertyEnabledBrokenCase() throws Exception {
		database.command(
				new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2, prop1) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		OIndex index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");
		Assert.assertNotNull(index);

		database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndextestclasS.pRoP1 FORCE")).execute();
		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");

		Assert.assertNull(index);
	}

	@Test
	public void testForcePropertyDisabled() throws Exception {
		database.command(
				new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1, prop2) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		OIndex index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");
		Assert.assertNotNull(index);

		try {
			database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndexTestClass.prop1")).execute();
			Assert.fail();
		} catch (OCommandExecutionException e) {
			Assert.assertEquals(e.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
					+ "). Please drop these indexes before removing property or use FORCE parameter.");
		}

		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");

		Assert.assertNotNull(index);

		final OIndexDefinition indexDefinition = index.getDefinition();

		Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
		Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
		Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
		Assert.assertEquals(index.getType(), "UNIQUE");
	}

	@Test
	public void testForcePropertyDisabledBrokenCase() throws Exception {
		database.command(
				new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1, prop2) UNIQUE"))
				.execute();

		try {
			database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndextestclaSS.proP1")).execute();
			Assert.fail();
		} catch (OCommandExecutionException e) {
			Assert.assertEquals(e.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
					+ "). Please drop these indexes before removing property or use FORCE parameter.");
		}

		database.getMetadata().getIndexManager().reload();
		final OIndex index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
				.getClassIndex("DropPropertyIndexCompositeIndex");

		Assert.assertNotNull(index);

		final OIndexDefinition indexDefinition = index.getDefinition();

		Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
		Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
		Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
		Assert.assertEquals(index.getType(), "UNIQUE");
	}
}
