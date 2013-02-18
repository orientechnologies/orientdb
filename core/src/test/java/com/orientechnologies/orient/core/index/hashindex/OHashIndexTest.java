package com.orientechnologies.orient.core.index.hashindex;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test
public class OHashIndexTest {
  private ODatabaseDocumentTx db;

  @BeforeMethod
  public void setUp() throws Exception {

    db = new ODatabaseDocumentTx("local:hashIndexTest");

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    db.close();
  }

  @Test(enabled = false)
  public void testCreate() throws Exception {
    final OClass oClass = db.getMetadata().getSchema().createClass("testClass");
    oClass.createProperty("name", OType.STRING);
    final OIndex<?> index = oClass.createIndex("testClassNameIndex", OClass.INDEX_TYPE.HASH, "name");

    Assert.assertNotNull(index);
  }
}
