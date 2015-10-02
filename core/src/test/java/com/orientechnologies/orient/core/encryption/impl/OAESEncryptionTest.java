package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OInvalidStorageEncryptionKeyException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

/**
 * @author giastfader@github
 * @author Luca Garulli
 * @since 22.04.2015
 */
@Test
public class OAESEncryptionTest extends AbstractEncryptionTest {
  private static final String DBNAME_DATABASETEST = "testCreatedAESEncryptedDatabase";
  private static final String DBNAME_CLUSTERTEST  = "testCreatedAESEncryptedCluster";

  public void testOAESEncryptedCompressionNoKey() {
    try {
      testEncryption(OAESEncryption.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testOAESEncryptedInvalidKey() {
    try {
      testEncryption(OAESEncryption.NAME, "ee");
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testOAESEncrypted() {
    testEncryption(OAESEncryption.NAME, "T1JJRU5UREJfSVNfQ09PTA==");
  }

  public void testCreatedAESEncryptedDatabase() {
    String buildDirectory = System.getProperty("buildDirectory", ".");

    final String dbPath = buildDirectory + File.separator + DBNAME_DATABASETEST;
    OFileUtils.deleteRecursively(new File(dbPath));

    final ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

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
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
        storage.close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
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
      db.activateOnCurrentThread();
      if (db.isClosed())
        db.open("admin", "admin");

      db.drop();
    }
  }

  public void testCreatedAESEncryptedCluster() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbPath = buildDirectory + File.separator + DBNAME_CLUSTERTEST;

    OFileUtils.deleteRecursively(new File(dbPath));
    final ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();
    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("alter cluster TestEncryption encryption aes")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      OStorage storage = ((ODatabaseDocumentInternal) db).getStorage();

      db.close();

      storage.close(true, false);

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

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        storage = ((ODatabaseDocumentInternal) db).getStorage();
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.close();
        storage.close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
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
