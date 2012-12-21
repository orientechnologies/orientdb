package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin
 * @since 21.12.12
 */
@Test
public class TestCleanOut {
  private ODatabaseDocumentTx db;

  @Parameters(value = "url")
  public TestCleanOut(String url) {
    db = new ODatabaseDocumentTx(url);
  }

  @BeforeMethod
  public void beforeMethod() {
    db.open("admin", "admin");
    initSchema();
  }

  @AfterMethod
  public void afterMethod() {
    db.close();
  }

  public void testRecordCleanOut() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final ODocument document = new ODocument("TestCleanOut");
    document.field("prop", "propvalue1");
    document.save();

    final OIndex index = db.getMetadata().getIndexManager().getIndex("TestCleanUpIndex");
    Assert.assertEquals(index.getSize(), 1);
    Assert.assertTrue(index.contains("propvalue1"));

    db.cleanOutRecord(document.getIdentity(), document.getRecordVersion());

    Assert.assertEquals(index.getSize(), 0);

    Assert.assertNull(db.load(document.getIdentity(), "*:0", false, true));
  }

  public void testTombstoneCleanOut() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODocument document = new ODocument("TestCleanOut");
    document.field("prop", "propvalue1");
    document.save();

    document = new ODocument("TestCleanOut");
    document.field("prop", "propvalue2");
    document.save();

    final OIndex index = db.getMetadata().getIndexManager().getIndex("TestCleanUpIndex");
    Assert.assertEquals(index.getSize(), 2);
    Assert.assertTrue(index.contains("propvalue1"));
    Assert.assertTrue(index.contains("propvalue2"));

    document.delete();

    Assert.assertEquals(index.getSize(), 1);
    Assert.assertTrue(index.contains("propvalue1"));

    document = db.load(document.getIdentity(), "*:0", false, true);

    db.cleanOutRecord(document.getIdentity(), document.getRecordVersion());

    Assert.assertEquals(index.getSize(), 1);
    Assert.assertTrue(index.contains("propvalue1"));

    Assert.assertNull(db.load(document.getIdentity(), "*:0", false, true));
  }

  private void initSchema() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final OMetadata metadata = db.getMetadata();
    final OSchema schema = metadata.getSchema();
    if (schema.existsClass("TestCleanOut"))
      schema.dropClass("TestCleanOut");

    final OClass cleanUpClass = schema.createClass("TestCleanOut");
    cleanUpClass.createProperty("prop", OType.STRING);
    cleanUpClass.createIndex("TestCleanUpIndex", OClass.INDEX_TYPE.UNIQUE, "prop");
  }
}
