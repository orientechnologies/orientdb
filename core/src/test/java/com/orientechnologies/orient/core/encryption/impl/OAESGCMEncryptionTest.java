package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.List;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.STORAGE_ENCRYPTION_KEY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Skymatic / Markus Kreusch (markus.kreusch--(at)--skymatic.de)
 */
public class OAESGCMEncryptionTest extends AbstractEncryptionTest {

  private static final String DBNAME_DATABASETEST = "testCreatedAESGCMEncryptedDatabase";
  private static final String DBNAME_CLUSTERTEST  = "testCreatedAESGCMEncryptedCluster";

  @Rule
  public ExpectedException    expectedException   = ExpectedException.none();

  @Test
  public void testOAESGCMEncryptionWithoutKey() {
    expectedException.expect(OSecurityException.class);
    expectedException.expectMessage("no key");

    testEncryption(OAESGCMEncryption.NAME);
  }

  @Test
  public void testOAESEncryptedInvalidKeyDueToInvalidBase64() {
    expectedException.expect(OInvalidStorageEncryptionKeyException.class);

    testEncryption(OAESGCMEncryption.NAME, "T1JJRU:UREJfSVNf;09PTF9TT#9DT09M");
  }

  @Test
  public void testOAESEncryptedInvalidKeyDueToInvalidKeySize() {
    expectedException.expect(OInvalidStorageEncryptionKeyException.class);

    testEncryption(OAESGCMEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTF9TT19DT09MX1NP");
  }

  @Test
  public void testOAESEncryptedWith128BitKey() {
    assertTrue(testEncryption(OAESGCMEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTA=="));
  }

  @Test
  @Ignore
  public void testOAESEncryptedWith192BitKey() {
    assertTrue(testEncryption(OAESGCMEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTF9TT19DT09M"));
  }

  @Test
  @Ignore
  public void testOAESEncryptedWith256BitKey() {
    assertTrue(testEncryption(OAESGCMEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTF9TT19DT09MX1NPX0NPT0w="));
  }

  @Test
  public void testCreatedAESEncryptedDatabase() {
    String buildDirectory = System.getProperty("buildDirectory", ".");

    final String dbPath = buildDirectory + File.separator + DBNAME_DATABASETEST;
    OFileUtils.deleteRecursively(new File(dbPath));
    OrientDBConfigBuilder builder = OrientDBConfig.builder();
    builder.addConfig(STORAGE_ENCRYPTION_METHOD, "aes/gcm");
    builder.addConfig(STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==");
    OrientDB orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());
    orientDB.create(DBNAME_DATABASETEST, ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(DBNAME_DATABASETEST, "admin", "admin");
    try {
      db.command("create class TestEncryption");
      db.command("insert into TestEncryption set name = 'Jay'");

      try (OResultSet result = db.query("select from TestEncryption")) {
        Assert.assertEquals(result.stream().count(), 1);
      }
      db.close();
      orientDB.close();

      orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());
      db = orientDB.open(DBNAME_DATABASETEST, "admin", "admin");
      try (OResultSet result = db.query("select from TestEncryption")) {
        Assert.assertEquals(result.stream().count(), 1);
      }
      orientDB.close();

      builder = OrientDBConfig.builder();
      builder.addConfig(STORAGE_ENCRYPTION_METHOD, "aes/gcm");
      builder.addConfig(STORAGE_ENCRYPTION_KEY, "invalidPassword");
      orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());
      Exception exception = null;
      try {
        orientDB.open(DBNAME_DATABASETEST, "admin", "admin");
      } catch (OSecurityException e) {
        exception = e;
      } finally {
        assertNotNull(exception);
      }
      orientDB.close();

      builder = OrientDBConfig.builder();
      builder.addConfig(STORAGE_ENCRYPTION_METHOD, "aes/gcm");
      builder.addConfig(STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA=-");
      orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());

      exception = null;
      try {
        orientDB.open(DBNAME_DATABASETEST, "admin", "admin");
      } catch (Exception e) {
        exception = e;
      } finally {
        assertNotNull(exception);
      }

      orientDB.close();

      builder = OrientDBConfig.builder();
      builder.addConfig(STORAGE_ENCRYPTION_METHOD, "aes/gcm");
      builder.addConfig(STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==");
      orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());
      db = orientDB.open(DBNAME_DATABASETEST, "admin", "admin");
      try (OResultSet result = db.query("select from TestEncryption")) {
        Assert.assertEquals(result.stream().count(), 1);
      }
      orientDB.close();
    } finally {
      builder = OrientDBConfig.builder();
      builder.addConfig(STORAGE_ENCRYPTION_METHOD, "aes/gcm");
      builder.addConfig(STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==");
      orientDB = new OrientDB("embedded:" + buildDirectory, builder.build());
      orientDB.drop(DBNAME_DATABASETEST);
      orientDB.close();
    }
  }

  @Test
  @Ignore
  public void testCreatedAESEncryptedCluster() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbPath = buildDirectory + File.separator + DBNAME_CLUSTERTEST;

    OFileUtils.deleteRecursively(new File(dbPath));
    final ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

    db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();
    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("alter class TestEncryption encryption aes")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      OStorage storage = ((ODatabaseDocumentInternal) db).getStorage();

      OrientDBInternal orientDB = ((ODatabaseDocumentTx) db).getSharedContext().getOrientDB();
      db.close();

      orientDB.forceDatabaseClose(db.getName());
//      storage.close(true, false);

      db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      storage = ((ODatabaseDocumentInternal) db).getStorage();
      db.close();

//      storage.close(true, false);
      orientDB.forceDatabaseClose(db.getName());

      db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
      OSecurityException exception = null;
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

        db.query(new OSQLSynchQuery<ODocument>("select from OUser"));
      } catch (OSecurityException e) {
        exception = e;
      } finally {
        db.close();
//        storage.close(true, false);
        orientDB.forceDatabaseClose(db.getName());
        assertNotNull(exception);
      }

      db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      exception = null;
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      } catch (OSecurityException e) {
        exception = e;
      } finally {
        db.activateOnCurrentThread();
        db.close();
//        storage.close(true, false);
        orientDB.forceDatabaseClose(db.getName());
        assertNotNull(exception);
      }

      db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      db.activateOnCurrentThread();
      if (db.isClosed())
        db.open("admin", "admin");

      db.drop();
    }
  }
}
