package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
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
  private String url;

  @Parameters(value = "url")
  public ReplicaPlacementTest(String iURL) {
    url = iURL;
  }

  public void testReplicaAddition() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaAddition").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaAdditionTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaAdditionTombstone").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaReplacementSuccess() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaReplacementSuccess").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaReplacementFail() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaReplacementFail").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaReplacementTombstoneToRecord() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaReplacementTombstoneToRecord").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaReplacementRecordToTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaReplacementRecordToTombstone").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  public void testReplicaReplacementTombstoneToTombstone() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    InitScheme initScheme = new InitScheme("ReplicaReplacementTombstoneToTombstone").invoke();
    ODatabaseDocumentTx dbOne = initScheme.getDbOne();
    ODatabaseDocumentTx dbTwo = initScheme.getDbTwo();

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

    dbOne.close();
    dbTwo.close();
  }

  private String getDBUrl(String dbName) {
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

  private class InitScheme {
    private ODatabaseDocumentTx dbOne;
    private ODatabaseDocumentTx dbTwo;
    private final String        dbName;

    private InitScheme(String dbName) {
      this.dbName = dbName;
    }

    public ODatabaseDocumentTx getDbOne() {
      return dbOne;
    }

    public ODatabaseDocumentTx getDbTwo() {
      return dbTwo;
    }

    public InitScheme invoke() {
      dbOne = new ODatabaseDocumentTx(getDBUrl(dbName + "One"));
      dbOne.create();

      final OSchema schemaOne = dbOne.getMetadata().getSchema();
      final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
      classReplicaTestOne.createProperty("prop", OType.STRING);
      classReplicaTestOne.createIndex("propindex", OClass.INDEX_TYPE.UNIQUE, "prop");
      schemaOne.save();

      dbTwo = new ODatabaseDocumentTx(getDBUrl(dbName + "Two"));
      dbTwo.create();

      final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
      final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
      classReplicaTestTwo.createProperty("prop", OType.STRING);
      classReplicaTestTwo.createIndex("propindex", OClass.INDEX_TYPE.UNIQUE, "prop");
      schemaTwo.save();
      return this;
    }
  }
}
