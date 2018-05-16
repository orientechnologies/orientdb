package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PaginatedClusterRollbackIT;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Set;

public class SBTreeWALRollbackIT {
  public static  String buildDirectory;
  private static String orientDirectory;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    orientDirectory = buildDirectory + File.separator + PaginatedClusterRollbackIT.class.getName();
    OFileUtils.deleteRecursively(new File(orientDirectory));
  }

  @AfterClass
  public static void afterClass() {
    OFileUtils.deleteRecursively(new File(orientDirectory));
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
        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
        cls.createIndex("secondary_value_index", OClass.INDEX_TYPE.NOTUNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testPut");
        documentOne.field("value", "a");
        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");
        final OIndex secondary_index = session.getMetadata().getIndexManager().getIndex("secondary_value_index");
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        Set<OIdentifiable> values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        final ODocument documentTwo = new ODocument("testPut");
        documentTwo.field("value", "a");

        try {
          documentTwo.save();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(1, secondary_index.getKeySize());

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        if (documentTwo.getIdentity().isPersistent()) {
          Assert.assertNull(session.load(documentTwo.getIdentity()));
        }
      }
    }
  }

  @Test
  public void testPutCamelCase() {
    final String dbName = "testPutCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testPut");
        cls.createProperty("value", OType.STRING);
        cls.createIndex("ValueIndex", OClass.INDEX_TYPE.UNIQUE, "value");
        cls.createIndex("SecondaryValueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testPut");
        documentOne.field("value", "a");
        documentOne.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("ValueIndex");
        final OIndex secondary_index = session.getMetadata().getIndexManager().getIndex("SecondaryValueIndex");
        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        Set<OIdentifiable> values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        final ODocument documentTwo = new ODocument("testPut");
        documentTwo.field("value", "a");

        try {
          documentTwo.save();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(1, primary_index.getKeySize());
        Assert.assertEquals(1, secondary_index.getKeySize());

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));

        values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        if (documentTwo.getIdentity().isPersistent()) {
          Assert.assertNull(session.load(documentTwo.getIdentity()));
        }
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
        cls.createIndex("value_index", OClass.INDEX_TYPE.UNIQUE, "value");
        cls.createIndex("secondary_value_index", OClass.INDEX_TYPE.NOTUNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testRemove");
        documentOne.field("value", "a");
        documentOne.save();

        ODocument document = new ODocument("testRemove");
        document.field("value", "c");
        document.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("value_index");
        final OIndex secondary_index = session.getMetadata().getIndexManager().getIndex("secondary_value_index");

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));
        Set<OIdentifiable> values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        Assert.assertEquals(document.getIdentity(), primary_index.get("c"));
        values = (Set<OIdentifiable>) secondary_index.get("c");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(document.getIdentity()));

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());

        session.begin();

        final ODocument documentTwo = session.load(document.getIdentity());
        documentTwo.delete();

        final ODocument documentThree = new ODocument("testRemove");
        documentThree.field("value", "a");
        documentThree.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));
        values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        Assert.assertEquals(document.getIdentity(), primary_index.get("c"));
        values = (Set<OIdentifiable>) secondary_index.get("c");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(document.getIdentity()));

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());
      }
    }
  }

  @Test
  public void testRemoveCamelCase() {
    final String dbName = "testRemoveCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final OMetadata metadata = session.getMetadata();
        final OSchema schema = metadata.getSchema();
        final OClass cls = schema.createClass("testRemove");
        cls.createProperty("value", OType.STRING);
        cls.createIndex("ValueIndex", OClass.INDEX_TYPE.UNIQUE, "value");
        cls.createIndex("SecondaryValueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "value");
      }

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final ODocument documentOne = new ODocument("testRemove");
        documentOne.field("value", "a");
        documentOne.save();

        ODocument document = new ODocument("testRemove");
        document.field("value", "c");
        document.save();

        final OIndex primary_index = session.getMetadata().getIndexManager().getIndex("ValueIndex");
        final OIndex secondary_index = session.getMetadata().getIndexManager().getIndex("SecondaryValueIndex");

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));
        Set<OIdentifiable> values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        Assert.assertEquals(document.getIdentity(), primary_index.get("c"));
        values = (Set<OIdentifiable>) secondary_index.get("c");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(document.getIdentity()));

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());

        session.begin();

        final ODocument documentTwo = session.load(document.getIdentity());
        documentTwo.delete();

        final ODocument documentThree = new ODocument("testRemove");
        documentThree.field("value", "a");
        documentThree.save();

        try {
          session.commit();
          Assert.fail();
        } catch (Exception e) {
          //ignore
        }

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());

        Assert.assertEquals(documentOne.getIdentity(), primary_index.get("a"));
        values = (Set<OIdentifiable>) secondary_index.get("a");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(documentOne.getIdentity()));

        Assert.assertEquals(document.getIdentity(), primary_index.get("c"));
        values = (Set<OIdentifiable>) secondary_index.get("c");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(document.getIdentity()));

        Assert.assertEquals(2, primary_index.getKeySize());
        Assert.assertEquals(2, secondary_index.getKeySize());
      }
    }
  }
}
