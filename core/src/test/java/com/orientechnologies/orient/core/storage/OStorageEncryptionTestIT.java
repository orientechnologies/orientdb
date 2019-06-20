package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class OStorageEncryptionTestIT {
  @Test
  public void testEncryption() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbDirectory = buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName();
    final File dbDirectoryFile = new File(dbDirectory);
    OFileUtils.deleteRecursively(dbDirectoryFile);

    final OrientDBConfig orientDBConfig = OrientDBConfig.builder()
        .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==").build();
    try (final OrientDB orientDB = new OrientDB("embedded:" + dbDirectoryFile.getAbsolutePath(), orientDBConfig)) {
      orientDB.create("encryption", ODatabaseType.PLOCAL);
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
          document.setProperty("value", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
              + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,"
              + " quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. ");
          document.save();
        }

        final Random random = ThreadLocalRandom.current();
        for (int i = 0; i < 1_000; i++) {
          try (OResultSet resultSet = session.query("select from EncryptedData where id = ?", random.nextInt(10_000_000))) {
            if (resultSet.hasNext()) {
              final OResult result = resultSet.next();
              result.getElement().ifPresent(ORecord::delete);
            }
          }
        }
      }
    }

    try (final OrientDB orientDB = new OrientDB(
        "embedded:" + buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName(),
        OrientDBConfig.defaultConfig())) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        //ignore
      }
    }

    final OrientDBConfig wrongKeyOneOrientDBConfig = OrientDBConfig.builder()
        .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8=").build();
    try (final OrientDB orientDB = new OrientDB(
        "embedded:" + buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName(),
        wrongKeyOneOrientDBConfig)) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        //ignore
      }
    }

    final OrientDBConfig wrongKeyTwoOrientDBConfig = OrientDBConfig.builder()
        .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8").build();
    try (final OrientDB orientDB = new OrientDB(
        "embedded:" + buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName(),
        wrongKeyTwoOrientDBConfig)) {
      try {
        try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        //ignore
      }
    }

    try (final OrientDB orientDB = new OrientDB(
        "embedded:" + buildDirectory + File.separator + OStorageEncryptionTestIT.class.getSimpleName(), orientDBConfig)) {
      try (final ODatabaseSession session = orientDB.open("encryption", "admin", "admin")) {
        final OIndexManager indexManager = session.getMetadata().getIndexManager();
        final OIndex treeIndex = indexManager.getIndex("EncryptedTree");
        final OIndex hashIndex = indexManager.getIndex("EncryptedHash");

        for (final ODocument document : session.browseClass("EncryptedData")) {
          final int id = document.getProperty("id");
          final ORID treeRid = (ORID) treeIndex.get(id);
          final ORID hashRid = (ORID) hashIndex.get(id);

          Assert.assertEquals(document.getIdentity(), treeRid);
          Assert.assertEquals(document.getIdentity(), hashRid);
        }

        Assert.assertEquals(session.countClass("EncryptedData"), treeIndex.getSize());
        Assert.assertEquals(session.countClass("EncryptedData"), hashIndex.getSize());
      }
    }

  }

}
