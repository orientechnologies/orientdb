package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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

    final ODatabaseDocumentTx dbOne = new ODatabaseDocumentTx(getDBUrl("replicaAddOne"));
    dbOne.create();

    final OSchema schemaOne = dbOne.getMetadata().getSchema();
    final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
    classReplicaTestOne.createProperty("prop", OType.STRING);
    schemaOne.save();

    final ODatabaseDocumentTx dbTwo = new ODatabaseDocumentTx(getDBUrl("replicaAddTwo"));
    dbTwo.create();

    final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
    final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
    classReplicaTestTwo.createProperty("prop", OType.STRING);
    schemaTwo.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ODocument docOne = new ODocument("ReplicaTest");
    docOne.field("prop", "value");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    final ODocument docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne));
  }

  public void testReplicaReplacementSuccess() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final ODatabaseDocumentTx dbOne = new ODatabaseDocumentTx(getDBUrl("replicaReplacementSuccess1"));
    dbOne.create();

    final OSchema schemaOne = dbOne.getMetadata().getSchema();
    final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
    classReplicaTestOne.createProperty("prop", OType.STRING);
    schemaOne.save();

    final ODatabaseDocumentTx dbTwo = new ODatabaseDocumentTx(getDBUrl("replicaReplacementSuccess2"));
    dbTwo.create();

    final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
    final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
    classReplicaTestTwo.createProperty("prop", OType.STRING);
    schemaTwo.save();

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
  }

  public void testReplicaReplacementFail() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final ODatabaseDocumentTx dbOne = new ODatabaseDocumentTx(getDBUrl("replicaReplacementFail1"));
    dbOne.create();

    final OSchema schemaOne = dbOne.getMetadata().getSchema();
    final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
    classReplicaTestOne.createProperty("prop", OType.STRING);
    schemaOne.save();

    final ODatabaseDocumentTx dbTwo = new ODatabaseDocumentTx(getDBUrl("replicaReplacementFail2"));
    dbTwo.create();

    final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
    final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
    classReplicaTestTwo.createProperty("prop", OType.STRING);
    schemaTwo.save();

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
  }

  public void testReplicaReplacementTombstoneToRecord() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    final ODatabaseDocumentTx dbOne = new ODatabaseDocumentTx(getDBUrl("ReplicaReplacementTombstoneToRecordOne"));
    dbOne.create();

    final OSchema schemaOne = dbOne.getMetadata().getSchema();
    final OClass classReplicaTestOne = schemaOne.createClass("ReplicaTest");
    classReplicaTestOne.createProperty("prop", OType.STRING);
    schemaOne.save();

    final ODatabaseDocumentTx dbTwo = new ODatabaseDocumentTx(getDBUrl("ReplicaReplacementTombstoneToRecordTwo"));
    dbTwo.create();

    final OSchema schemaTwo = dbTwo.getMetadata().getSchema();
    final OClass classReplicaTestTwo = schemaTwo.createClass("ReplicaTest");
    classReplicaTestTwo.createProperty("prop", OType.STRING);
    schemaTwo.save();

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
}
