package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.testng.annotations.*;

import static org.testng.Assert.assertTrue;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class OObjectEntitySerializerTest {

	private OObjectDatabaseTx databaseTx;

	@BeforeClass
	protected void setUp() throws Exception {
		databaseTx = new OObjectDatabaseTx("memory:OObjectEntitySerializerTest");
		databaseTx.create();

		databaseTx.getEntityManager().registerEntityClass(ExactEntity.class);

	}

	@AfterClass
	protected void tearDown() {
		databaseTx.close();
	}

	@Test
  public void testCallbacksHierarchy() {
		ExactEntity entity = new ExactEntity();
		databaseTx.save(entity);

    assertTrue(entity.callbackExecuted());
  }

	@Test
	public void testCallbacksHierarchyUpdate() {
		ExactEntity entity = new ExactEntity();
		entity = databaseTx.save(entity);

		entity.reset();
		databaseTx.save(entity);
		assertTrue(entity.callbackExecuted());
	}
}
