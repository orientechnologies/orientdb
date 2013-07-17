package com.orientechnologies.orient.test.database.auto;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

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
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
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

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value"));
  }

  public void testReplicaAdditionRecord() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    final ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    final ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);

    Assert.assertEquals(recordTwo.toStream(), recordOne.toStream());
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

  public void testReplicaAdditionTombstoneRecord() {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();
    recordOne.delete();

    recordOne = dbOne.load(recordOne.getIdentity(), "*:0", false, true);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    final ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity(), "*:0", false, true);
    Assert.assertNotNull(recordTwo);

    Assert.assertEquals(recordOne.getRecordVersion(), recordTwo.getRecordVersion());
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

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    docOne.field("prop", "value2");
    docOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(docOne);
    docTwo = dbTwo.load(docOne.getIdentity());
    Assert.assertNotNull(docTwo);

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value2"));
  }

  public void testReplicaReplacementSuccessRecord() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);

    Assert.assertEquals(recordTwo.toStream(), recordOne.toStream());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 22 }));
    recordOne.save();
    recordOne = dbOne.load(recordOne.getIdentity());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);

    Assert.assertEquals(recordTwo.toStream(), recordOne.toStream());
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

    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

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

    Assert.assertFalse(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value4"));
  }

  public void testReplicaReplacementFailRecord() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);

    Assert.assertEquals(recordTwo.toStream(), recordOne.toStream());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 22 }));
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    recordTwo.setDirty();
    recordTwo.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 44 }));
    recordTwo.save();

    recordTwo.setDirty();
    recordTwo.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 55 }));
    recordTwo.save();

    final ORecordVersion oldVersion = recordTwo.getRecordVersion();

    Assert.assertFalse(dbTwo.updatedReplica(recordOne));
    recordTwo = dbTwo.load(recordOne.getIdentity());

    Assert.assertEquals(0, recordTwo.getRecordVersion().compareTo(oldVersion));
    Assert.assertEquals(recordTwo.toStream(), new byte[] { 10, 11, 55 });

    Assert.assertFalse(Arrays.equals(recordOne.toStream(), recordTwo.toStream()));
  }

  public void testReplicaReplacementTombstoneToDocument() {
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
    Assert.assertTrue(ODocumentHelper.hasSameContentItem(docTwo, dbTwo, docOne, dbOne, null));

    final OIndex propindex = dbTwo.getMetadata().getIndexManager().getIndex("propindex");
    Assert.assertEquals(1, propindex.getSize());
    Assert.assertTrue(propindex.contains("value5"));
  }

  public void testReplicaReplacementTombstoneToRecord() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);
    recordTwo.delete();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 22 }));
    recordOne.save();

    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 44 }));
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    recordTwo = dbTwo.load(recordOne.getIdentity());

    Assert.assertNotNull(recordTwo);
    Assert.assertEquals(recordTwo.toStream(), recordOne.toStream());
  }

  public void testReplicaReplacementDocumentToTombstone() {
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

  public void testReplicaReplacementRecordToTombstone() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 22 }));
    recordOne.save();
    recordOne.delete();
    recordOne = dbOne.load(recordOne.getIdentity(), "*:0", false, true);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    recordTwo = dbTwo.load(recordOne.getIdentity());

    Assert.assertNull(recordTwo);
  }

  public void testReplicaReplacementTombstoneDocumentToTombstone() {
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

  public void testReplicaReplacementTombstoneRecordToTombstone() throws Exception {
    if (!OGlobalConfiguration.USE_LHPEPS_MEMORY_CLUSTER.getValueAsBoolean())
      return;

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    ORecordBytes recordOne = new ORecordBytes(new byte[] { 10, 11, 22, 33, 44, 55, 66, 77 });
    recordOne.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    ORecordBytes recordTwo = dbTwo.load(recordOne.getIdentity());
    Assert.assertNotNull(recordTwo);
    recordTwo.delete();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbOne);
    recordOne.setDirty();
    recordOne.fromInputStream(new ByteArrayInputStream(new byte[] { 10, 11, 22 }));
    recordOne.save();

    recordOne.delete();
    recordOne = dbOne.load(recordOne.getIdentity(), "*:0", false, true);

    ORecordVersion docOneVersion = recordOne.getRecordVersion();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbTwo);
    dbTwo.updatedReplica(recordOne);
    recordTwo = dbTwo.load(recordOne.getIdentity(), "*:0", false, true);

    Assert.assertEquals(docOneVersion, recordTwo.getRecordVersion());
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
