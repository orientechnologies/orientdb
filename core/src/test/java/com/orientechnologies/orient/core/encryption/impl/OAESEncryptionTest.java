package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

/**
 * @author giastfader@github
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since 22.04.2015
 */
public class OAESEncryptionTest extends AbstractEncryptionTest {
  private static final String DBNAME_DATABASETEST = "testCreatedAESEncryptedDatabase";
  private static final String DBNAME_CLUSTERTEST  = "testCreatedAESEncryptedCluster";

  @Test
  public void testOAESEncryptedCompressionNoKey() {
    try {
      testEncryption(OAESEncryption.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testOAESEncryptedInvalidKey() {
    try {
      testEncryption(OAESEncryption.NAME, "ee");
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testOAESEncrypted() {
    testEncryption(OAESEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTA==");
  }

  @Test
  public void testCreatedAESEncryptedDatabase() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");

    final String dbPath = buildDirectory + File.separator + DBNAME_DATABASETEST;
    OFileUtils.deleteRecursively(new File(dbPath));

    ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();

    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      OStorage storage = ((ODatabaseDocumentInternal) db).getStorage();
      db.close();

      storage.close(true, false);

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      storage = ((ODatabaseDocumentInternal) db).getStorage();
      db.close();

      storage.close(true, false);

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
      try {
        db.open("admin", "admin");
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }

      closeStorage(dbPath);

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "aes");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      closeStorage(dbPath);
      OFileUtils.deleteRecursively(new File(dbPath));
    }
  }

  private void closeStorage(String dbPath) throws IOException {
    final Collection<OStorage> storages = Orient.instance().getStorages();
    for (OStorage stg : storages) {
      if (stg instanceof OLocalPaginatedStorage) {
        OLocalPaginatedStorage paginatedStorage = (OLocalPaginatedStorage) stg;
        if (!stg.isClosed()) {
          if (paginatedStorage.getStoragePath().toRealPath().equals(Paths.get(dbPath).toRealPath())) {
            stg.close(true, false);
          }
        }
      }
    }
  }

  @Test
  public void testCreatedAESEncryptedCluster() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbPath = buildDirectory + File.separator + DBNAME_CLUSTERTEST;

    OFileUtils.deleteRecursively(new File(dbPath));
    ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

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

      db.close();

      storage.close(true, false);

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      storage = ((ODatabaseDocumentInternal) db).getStorage();
      db.close();

      storage.close(true, false);

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

        result = db.query(new OSQLSynchQuery<ODocument>("select from OUser"));
        Assert.assertFalse(result.isEmpty());

        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.close();
        storage.close(true, false);
      }

      db = new ODatabaseDocumentTx("plocal:" + dbPath);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
        storage.close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      closeStorage(dbPath);
      OFileUtils.deleteRecursively(new File(dbPath));
    }
  }
}
