package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = { "index" })
public class SQLDropIndexTest {

	private final ODatabaseDocumentTx	database;
	private static final OType				EXPECTED_PROP1_TYPE	= OType.DOUBLE;
	private static final OType				EXPECTED_PROP2_TYPE	= OType.INTEGER;

	@Parameters(value = "url")
	public SQLDropIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeClass
	public void beforeClass() {
		if (database.isClosed())
			database.open("admin", "admin");

		final OSchema schema = database.getMetadata().getSchema();
		final OClass oClass = schema.createClass("SQLDropIndexTestClass");
		oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
		oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);

		schema.save();
	}

	@AfterClass
	public void afterClass() throws Exception {
		if (database.isClosed())
			database.open("admin", "admin");
		database.command(new OCommandSQL("delete from SQLDropIndexTestClass")).execute();
		database.command(new OCommandSQL("drop class SQLDropIndexTestClass")).execute();
		database.reload();
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

	@Test
	public void testOldSyntax() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX SQLDropIndexTestClass.prop1 UNIQUE")).execute();

		database.getMetadata().getIndexManager().reload();

		OIndex<?> index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass")
				.getClassIndex("SQLDropIndexTestClass.prop1");
		Assert.assertNotNull(index);

		database.command(new OCommandSQL("DROP INDEX SQLDropIndexTestClass.prop1")).execute();
		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass").getClassIndex("SQLDropIndexTestClass.prop1");
		Assert.assertNull(index);
	}

	@Test(dependsOnMethods = "testOldSyntax")
	public void testDropIndexWithoutClass() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX SQLDropIndexWithoutClass UNIQUE double")).execute();
		database.getMetadata().getIndexManager().reload();

		OIndex<?> index = database.getMetadata().getIndexManager().getIndex("SQLDropIndexWithoutClass");
		Assert.assertNotNull(index);

		database.command(new OCommandSQL("DROP INDEX SQLDropIndexWithoutClass")).execute();
		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getIndexManager().getIndex("SQLDropIndexWithoutClass");

		Assert.assertNull(index);

	}

	@Test(dependsOnMethods = "testDropIndexWithoutClass")
	public void testDropCompositeIndex() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX SQLDropIndexCompositeIndex ON SQLDropIndexTestClass (prop1, prop2) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		OIndex<?> index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass")
				.getClassIndex("SQLDropIndexCompositeIndex");
		Assert.assertNotNull(index);

		database.command(new OCommandSQL("DROP INDEX SQLDropIndexCompositeIndex")).execute();
		database.getMetadata().getIndexManager().reload();

		index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass").getClassIndex("SQLDropIndexCompositeIndex");
		Assert.assertNull(index);
	}

	@Test(dependsOnMethods = "testDropCompositeIndex")
	public void testDropIndexWorkedCorrectly() {
		OIndex<?> index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass")
				.getClassIndex("SQLDropIndexTestClass.prop1");
		Assert.assertNull(index);
		index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass").getClassIndex("SQLDropIndexWithoutClass");
		Assert.assertNull(index);
		index = database.getMetadata().getSchema().getClass("SQLDropIndexTestClass").getClassIndex("SQLDropIndexCompositeIndex");
		Assert.assertNull(index);
	}

}
