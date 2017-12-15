package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
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
public class ODESEncryptionTest extends AbstractEncryptionTest {
  private static final String DBNAME_CLUSTERTEST  = "testCreatedDESEncryptedCluster";
  private static final String DBNAME_DATABASETEST = "testCreatedDESEncryptedDatabase";

  @Test
  public void testODESEncryptedCompressionNoKey() {
    try {
      testEncryption(ODESEncryption.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testODESEncryptedCompressionInvalidKey() {
    try {
      testEncryption(ODESEncryption.NAME, "no");
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testODESEncryptedCompression() {
    testEncryption(ODESEncryption.NAME, "T1JJRU5UREI=");
  }

  @Test
  public void testCreatedDESEncryptedDatabase() throws Exception {
    OFileUtils.deleteRecursively(new File("target/" + DBNAME_DATABASETEST));

    ODatabaseInternal db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "des");
    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();

    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      db.close();

      db.getStorage().close(true, false);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "des");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.getStorage().close(true, false);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "des");
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

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "des");
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

      closeStorage("target/" + DBNAME_DATABASETEST);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD.getKey(), "des");
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      closeStorage("target/" + DBNAME_DATABASETEST);
      OFileUtils.deleteRecursively(new File("target/" + DBNAME_DATABASETEST));
    }
  }

  @Test
  public void testCreatedDESEncryptedCluster() throws Exception {
    OFileUtils.deleteRecursively(new File("target/" + DBNAME_CLUSTERTEST));

    ODatabaseInternal db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();

    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("alter class TestEncryption encryption des")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      db.close();

      db.getStorage().close(true, false);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.getStorage().close(true, false);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
      try {
        db.open("admin", "admin");
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

        result = db.query(new OSQLSynchQuery<ODocument>("select from OUser"));
        Assert.assertFalse(result.isEmpty());

        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }

      closeStorage("target/" + DBNAME_CLUSTERTEST);

      db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);
      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      closeStorage("target/" + DBNAME_CLUSTERTEST);
      OFileUtils.deleteRecursively(new File("target/" + DBNAME_CLUSTERTEST));
    }
  }

  private void closeStorage(String dbPath) throws IOException {
    final Collection<OStorage> storages = Orient.instance().getStorages();
    for (OStorage stg : storages) {
      if (stg instanceof OLocalPaginatedStorage) {
        OLocalPaginatedStorage paginatedStorage = (OLocalPaginatedStorage) stg;
        if (!paginatedStorage.isClosed()) {
          if (paginatedStorage.getStoragePath().toRealPath().equals(Paths.get(dbPath).toRealPath())) {
            stg.close(true, false);
          }
        }
      }
    }
  }
}
