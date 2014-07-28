package com.orientechnologies.orient.core.metadata.schema;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class OClassImplTest {

  private ODatabaseDocumentTx db;

  @BeforeMethod
  public void setUp() {
    db = new ODatabaseDocumentTx("memory:" + OClassImplTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
    } else
      db.create();
  }

  @AfterMethod
  public void after() {
    db.close();
  }

	@AfterClass
	public void afterClass() {
		db.open("admin", "admin");
		db.drop();
	}

  /**
   * If class was not abstract and we call {@code setAbstract(false)} clusters should not be changed.
   * 
   * @throws Exception
   */
  @Test
  public void testSetAbstractClusterNotChanged() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createClass("Test1");
    final int oldClusterId = oClass.getDefaultClusterId();

    oClass.setAbstract(false);

    Assert.assertEquals(oClass.getDefaultClusterId(), oldClusterId);
  }

  /**
   * If class was abstract and we call {@code setAbstract(false)} a new non default cluster should be created.
   * 
   * @throws Exception
   */
  @Test
  public void testSetAbstractShouldCreateNewClusters() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createAbstractClass("Test2");

    oClass.setAbstract(false);

    Assert.assertFalse(oClass.getDefaultClusterId() == -1);
    Assert.assertFalse(oClass.getDefaultClusterId() == db.getDefaultClusterId());
  }
}
