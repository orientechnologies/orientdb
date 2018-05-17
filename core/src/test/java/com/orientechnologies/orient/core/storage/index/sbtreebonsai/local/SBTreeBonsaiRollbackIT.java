package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class SBTreeBonsaiRollbackIT {
  public static  String buildDirectory;
  private static String orientDirectory;

  private int topThreshold;
  private int bottomThreshold;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    orientDirectory = buildDirectory + File.separator + SBTreeBonsaiRollbackIT.class.getName();
    OFileUtils.deleteRecursively(new File(orientDirectory));
  }

  @AfterClass
  public static void afterClass() {
    OFileUtils.deleteRecursively(new File(orientDirectory));
  }

  @Before
  public void beforeMethod() {
    topThreshold = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testPut() {
    final String dbName = "testPut";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testPut");
        cls.createProperty("value", OType.STRING);
        cls.createProperty("ridBag", OType.LINKBAG);
        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testPut");
        documentOne.field("value", "a");

        ORidBag ridBag = new ORidBag();
        ridBag.add(new ORecordId(1, 1));
        documentOne.field("ridBag", ridBag);
        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        documentOne.reload(null, true);

        session.begin();

        final ODocument documentTwo = new ODocument("testPut");
        documentTwo.field("value", "a");
        documentTwo.save();

        ridBag = documentOne.field("ridBag");
        ridBag.add(new ORecordId(2, 2));
        documentOne.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        if (documentTwo.getIdentity().isPersistent()) {
          Assert.assertNull(session.load(documentTwo.getIdentity()));
        }

        documentOne.reload(null, true);
        ridBag = documentOne.field("ridBag");

        Assert.assertEquals(1, ridBag.size());
        Assert.assertTrue(ridBag.contains(new ORecordId(1, 1)));
      }
    }

  }

  @Test
  public void testPutDuplicate() {
    final String dbName = "testPutDuplicate";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testPut");
        cls.createProperty("value", OType.STRING);
        cls.createProperty("ridBag", OType.LINKBAG);
        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testPut");
        documentOne.field("value", "a");

        ORidBag ridBag = new ORidBag();
        ridBag.add(new ORecordId(1, 1));
        documentOne.field("ridBag", ridBag);
        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        documentOne.reload(null, true);

        session.begin();

        final ODocument documentTwo = new ODocument("testPut");
        documentTwo.field("value", "a");
        documentTwo.save();

        ridBag = documentOne.field("ridBag");
        ridBag.add(new ORecordId(1, 1));
        documentOne.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        if (documentTwo.getIdentity().isPersistent()) {
          Assert.assertNull(session.load(documentTwo.getIdentity()));
        }

        documentOne.reload(null, true);
        ridBag = documentOne.field("ridBag");

        Assert.assertEquals(1, ridBag.size());
        Assert.assertTrue(ridBag.contains(new ORecordId(1, 1)));
      }
    }
  }

  @Test
  public void testRemove() {
    final String dbName = "testRemove";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testRemove");
        cls.createProperty("value", OType.STRING);
        cls.createProperty("ridBag", OType.LINKBAG);

        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testRemove");
        documentOne.field("value", "a");
        ORidBag ridBag = new ORidBag();
        ridBag.add(new ORecordId(1, 1));
        ridBag.add(new ORecordId(2, 2));
        documentOne.field("ridBag", ridBag);

        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(2, ridBag.size());

        documentOne.reload(null, true);
        session.begin();

        ridBag = documentOne.field("ridBag");
        ridBag.remove(new ORecordId(2, 2));
        documentOne.save();

        final ODocument documentTwo = new ODocument("testRemove");
        documentTwo.field("value", "a");
        documentTwo.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        documentOne.reload(null, true);
        ridBag = documentOne.field("ridBag");

        Assert.assertEquals(2, ridBag.size());
        Assert.assertTrue(ridBag.contains(new ORecordId(1, 1)));
        Assert.assertTrue(ridBag.contains(new ORecordId(2, 2)));
      }
    }
  }

  @Test
  public void testRemoveDuplicate() {
    final String dbName = "testRemoveDuplicate";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testRemove");
        cls.createProperty("value", OType.STRING);
        cls.createProperty("ridBag", OType.LINKBAG);

        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testRemove");
        documentOne.field("value", "a");
        ORidBag ridBag = new ORidBag();
        ridBag.add(new ORecordId(1, 1));
        ridBag.add(new ORecordId(1, 1));
        documentOne.field("ridBag", ridBag);

        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(2, ridBag.size());

        documentOne.reload(null, true);
        session.begin();

        ridBag = documentOne.field("ridBag");
        ridBag.remove(new ORecordId(1, 1));
        documentOne.save();

        final ODocument documentTwo = new ODocument("testRemove");
        documentTwo.field("value", "a");
        documentTwo.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        documentOne.reload(null, true);
        ridBag = documentOne.field("ridBag");

        Assert.assertEquals(2, ridBag.size());
        Assert.assertTrue(ridBag.contains(new ORecordId(1, 1)));
      }
    }
  }

}
