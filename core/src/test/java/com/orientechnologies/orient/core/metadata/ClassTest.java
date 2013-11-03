package com.orientechnologies.orient.core.metadata;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class ClassTest {
  private static ODatabaseDocumentTx db                   = null;
  public static final String         SHORTNAME_CLASS_NAME = "TestShortName";

  @BeforeMethod
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("memory:metadataclasstest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (!db.isClosed())
      db.close();
  }

  @Test
  public void testShortName() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass(SHORTNAME_CLASS_NAME);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    String shortName = "shortname";
    oClass.setShortName(shortName);
    Assert.assertEquals(shortName, oClass.getShortName());
    Assert.assertEquals(shortName, queryShortName());

    // FAILS, saves null value and stores "null" string (not null value) internally
    // shortName = "null";
    // oClass.setShortName(shortName);
    // Assert.assertEquals(shortName, oClass.getShortName());
    // Assert.assertEquals(shortName, queryShortName());

    oClass.setShortName(null);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    oClass.setShortName("");
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());
  }

  private String queryShortName() {
    String selectShortNameSQL = "select shortName from ( select flatten(classes) from cluster:internal )" + " where name = \""
        + SHORTNAME_CLASS_NAME + "\"";
    List<ODocument> result = db.command(new OCommandSQL(selectShortNameSQL)).execute();
    Assert.assertEquals(1, result.size());
    return result.get(0).field("shortName");
  }
}
