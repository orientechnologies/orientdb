package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = { "index" })
public class SchemaIndexTest {
	private final ODatabaseDocumentTx	database;

	@Parameters(value = "url")
	public SchemaIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");

		final OSchema schema = database.getMetadata().getSchema();
		final OClass superTest = schema.createClass("SchemaSharedIndexSuperTest");
		final OClass test = schema.createClass("SchemaIndexTest", superTest);
		test.createProperty("prop1", OType.DOUBLE);
		test.createProperty("prop2", OType.DOUBLE);

		schema.save();
	}

	@AfterMethod
	public void tearDown() throws Exception {
		database.command(new OCommandSQL("drop class SchemaIndexTest")).execute();
		database.command(new OCommandSQL("drop class SchemaSharedIndexSuperTest")).execute();
		database.getMetadata().getSchema().reload();
    database.getLevel2Cache().clear();


		database.close();
	}

	@Test
	public void testDropClass() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("SchemaSharedIndexCompositeIndex"));

		database.getMetadata().getSchema().dropClass("SchemaIndexTest");
		database.getMetadata().getSchema().reload();
		database.getMetadata().getIndexManager().reload();

		Assert.assertNull(database.getMetadata().getSchema().getClass("SchemaIndexTest"));
		Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

		Assert.assertNull(database.getMetadata().getIndexManager().getIndex("SchemaSharedIndexCompositeIndex"));
	}

	@Test
	public void testDropSuperClass() throws Exception {
		database.command(new OCommandSQL("CREATE INDEX SchemaSharedIndexCompositeIndex ON SchemaIndexTest (prop1, prop2) UNIQUE"))
				.execute();
		database.getMetadata().getIndexManager().reload();

		try {
			database.getMetadata().getSchema().dropClass("SchemaSharedIndexSuperTest");
			Assert.fail();
		} catch (OSchemaException e) {
			Assert
					.assertEquals(e.getMessage(),
							"Class SchemaSharedIndexSuperTest cannot be dropped because it has sub classes. Remove the dependencies before trying to drop it again");
		}

		database.getMetadata().getSchema().reload();

		Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaIndexTest"));
		Assert.assertNotNull(database.getMetadata().getSchema().getClass("SchemaSharedIndexSuperTest"));

		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("SchemaSharedIndexCompositeIndex"));
	}
}
