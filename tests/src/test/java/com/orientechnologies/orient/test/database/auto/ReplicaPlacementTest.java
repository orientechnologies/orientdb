package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 20.12.12
 */
@Test
public class ReplicaPlacementTest {
  private ODatabaseDocumentTx dbOne;
  private ODatabaseDocumentTx dbTwo;

  @Parameters(value = "url")
  public ReplicaPlacementTest(String iURL) {
    dbOne = new ODatabaseDocumentTx(getDBUrl("ReplicaPlacementTestOne", iURL));
    dbTwo = new ODatabaseDocumentTx(getDBUrl("ReplicaPlacementTestTwo", iURL));
  }

  @BeforeMethod
  public void beforeMethod() {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    if (!dbOne.exists())
      dbOne.create();
    else
      dbOne.open("admin", "admin");

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    if (!dbTwo.exists())
      dbTwo.create();
    else
      dbTwo.open("admin", "admin");

    initScheme();
  }

  @AfterMethod
  public void afterMethod() {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    dbOne.close();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.close();
  }

  public void testReplicaAddition() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    final ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value"));
  }

  public void testReplicaAdditionTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();
    docOne.delete();

    docOne = dbOne.load(docOne.getIdentity(), "*:0", false, true);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    final ODocument docTwo = dbTwo.load(docOne.getIdentity(), "*:0", false, true);
    Assert.assertNotNull(docTwo);

    Assert.assertEquals(docOne.getRecordVersion(), docTwo.getRecordVersion());

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(0, propindex.getSize());
  }

  public void testReplicaReplacementSuccess() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value2");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value2"));
  }

  public void testReplicaReplacementFail() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value2");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    docTwo.field("prop", "value3");
    docTwo.save();
    docTwo.field("prop", "value4");
    docTwo.save();

    final ORecordVersion oldVersion = docTwo.getRecordVersion();

    Assert.assertFalse(dbTwo.updatedReplica(docOne));
    docTwo = dbTwo.load(docOne.getIdentity());

    Assert.assertEquals(0, docTwo.getRecordVersion().compareTo(oldVersion));
    Assert.assertEquals("value4", docTwo.field("prop"));

    Assert.assertFalse(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value4"));
  }

  public void testReplicaReplacementTombstoneToRecord() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;
    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);
    docTwo.delete();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value3");
    docOne.save();
    docOne.field("prop", "value5");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    docTwo = dbTwo.load(docOne.getIdentity());

    Assert.assertNotNull(docTwo);
    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value5"));
  }

  public void testReplicaReplacementRecordToTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value3");
    docOne.save();
    docOne.delete();
    docOne = dbOne.load(docOne.getIdentity(), "*:0", false, true);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    docTwo = dbTwo.load(docOne.getIdentity());

    Assert.assertNull(docTwo);

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(0, propindex.getSize());
  }

  public void testReplicaReplacementTombstoneToTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);
    docTwo.delete();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value3");
    docOne.save();
    docOne.delete();
    docOne = dbOne.load(docOne.getIdentity(), "*:0", false, true);

    ORecordVersion docOneVersion = docOne.getRecordVersion();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    docTwo = dbTwo.load(docOne.getIdentity(), "*:0", false, true);

    Assert.assertEquals(docOneVersion, docTwo.getRecordVersion());

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(0, propindex.getSize());
  }

  private String getDBUrl(String dbName, String url) {
    int pos = url.lastIndexOf("/");
    final String u;

    if (pos > -1)
      u = url.substring(0, pos) + "/" + dbName;
    else {
      pos = url.lastIndexOf(":");
      u = url.substring(0, pos + 1) + "/" + dbName;
    }

    return u;

  }

  private void initScheme() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final OSchema schemaOne = dbOne.getMetadata().getSchema();
    if (schemaOne.existsClass("ReplicaTest"))
      schemaOne.dropClass("ReplicaTest");

    final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
    classReplicaTestOne.createProperty("prop", OType.STRING);
    classReplicaTestOne.createIndex("propindex", OClass.INDEX_TYPE.UNIQUE, "prop");
    schemaOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
    if (schemaTwo.existsClass("ReplicaTest"))
      schemaTwo.dropClass("ReplicaTest");

    final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
    classReplicaTestTwo.createProperty("prop", OType.STRING);
    classReplicaTestTwo.createIndex("propindex", OClass.INDEX_TYPE.UNIQUE, "prop");
    schemaTwo.save();
  }
}
