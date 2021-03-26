package com.orientechnologies.orient.core.storage;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class OStorageEncryptionTestIT {
  @Test
  public void testEncryption() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    final OrientDBConfig orientDBConfig =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    try (final OrientDB orientDB =
        new OrientDB("embedded:" + dbDirectoryFile.getAbsolutePath(), orientDBConfig)) {
      orientDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
      try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
        final OSchema schema = session.getMetadata().getSchema();
        final OClass cls = schema.createClass("EncryptedData");
        cls.createProperty("id", OType.INTEGER);
        cls.createProperty("value", OType.STRING);

        cls.createIndex("EncryptedTree", OClass.INDEX_TYPE.UNIQUE, "id");
        cls.createIndex("EncryptedHash", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "id");

        for (int i = 0; i < 10_000; i++) {
          final ODocument document = new ODocument(cls);
          document.setProperty("id", i);
          document.setProperty(
              "value",
              "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
                  + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,"
                  + " quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. ");
          document.save();
        }

        final Random random = ThreadLocalRandom.current();
        for (int i = 0; i < 1_000; i++) {
          try (OResultSet resultSet =
              session.query("select from EncryptedData where id = ?", random.nextInt(10_000_000))) {
            if (resultSet.hasNext()) {
              final OResult result = resultSet.next();
              result.getElement().ifPresent(ORecord::delete);
            }
          }
        }
      }
    }

    try (final OrientDB orientDB =
        new OrientDB(
            "embedded:" + dbDirectoryFile.getAbsolutePath(), OrientDBConfig.defaultConfig())) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final OrientDBConfig wrongKeyOneOrientDBConfig =
        OrientDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8=")
            .build();
    try (final OrientDB orientDB =
        new OrientDB("embedded:" + dbDirectoryFile.getAbsolutePath(), wrongKeyOneOrientDBConfig)) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final OrientDBConfig wrongKeyTwoOrientDBConfig =
        OrientDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8")
            .build();
    try (final OrientDB orientDB =
        new OrientDB("embedded:" + dbDirectoryFile.getAbsolutePath(), wrongKeyTwoOrientDBConfig)) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    try (final OrientDB orientDB =
        new OrientDB("embedded:" + dbDirectoryFile.getAbsolutePath(), orientDBConfig)) {
      try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
        final OIndexManagerAbstract indexManager =
            ((ODatabaseDocumentInternal) session).getMetadata().getIndexManagerInternal();
        final OIndex treeIndex =
            indexManager.getIndex((ODatabaseDocumentInternal) session, "EncryptedTree");
        final OIndex hashIndex =
            indexManager.getIndex((ODatabaseDocumentInternal) session, "EncryptedHash");

        for (final ODocument document : session.browseClass("EncryptedData")) {
          final int id = document.getProperty("id");
          final ORID treeRid;
          try (Stream<ORID> rids = treeIndex.getInternal().getRids(id)) {
            treeRid = rids.findFirst().orElse(null);
          }
          final ORID hashRid;
          try (Stream<ORID> rids = hashIndex.getInternal().getRids(id)) {
            hashRid = rids.findFirst().orElse(null);
          }

          Assert.assertEquals(document.getIdentity(), treeRid);
          Assert.assertEquals(document.getIdentity(), hashRid);
        }

        Assert.assertEquals(session.countClass("EncryptedData"), treeIndex.getInternal().size());
        Assert.assertEquals(session.countClass("EncryptedData"), hashIndex.getInternal().size());
      }
    }
  }

  private File cleanAndGetDirectory() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbDirectory =
        buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName();
    final File dbDirectoryFile = new File(dbDirectory);
    OFileUtils.deleteRecursively(dbDirectoryFile);
    return dbDirectoryFile;
  }

  @Test
  public void testEncryptionSingleDatabase() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    try (final OrientDB orientDB =
        new OrientDB(
            "embedded:" + dbDirectoryFile.getAbsolutePath(), OrientDBConfig.defaultConfig())) {
      final OrientDBConfig orientDBConfig =
          OrientDBConfig.builder()
              .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();

      orientDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
    }
    try (final OrientDB orientDB =
        new OrientDB(
            "embedded:" + dbDirectoryFile.getAbsolutePath(), OrientDBConfig.defaultConfig())) {
      final OrientDBConfig orientDBConfig =
          OrientDBConfig.builder()
              .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();
      try (final ODatabaseSession session =
          orientDB.open("encryption", "admin", "admin", orientDBConfig)) {
        final OSchema schema = session.getMetadata().getSchema();
        final OClass cls = schema.createClass("EncryptedData");

        final ODocument document = new ODocument(cls);
        document.setProperty("id", 10);
        document.setProperty(
            "value",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
                + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,"
                + " quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. ");
        document.save();

        try (OResultSet resultSet = session.query("select from EncryptedData where id = ?", 10)) {
          assertTrue(resultSet.hasNext());
        }
      }
    }
  }
}
