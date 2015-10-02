package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

/**
 * @author Luca Garulli
 * @since 22.04.2015
 */
@Test
public class ONothingEncryptionTest extends AbstractEncryptionTest {
  private static final String DBNAME_CLUSTERTEST  = "testCreatedNothingEncryptedCluster";
  private static final String DBNAME_DATABASETEST = "testCreatedNothingEncryptedDatabase";

  public void testONothingEncryptedCompressionNoKey() {
    testEncryption(ONothingEncryption.NAME);
  }

  public void testONothingEncryptedCompressionInvalidKey() {
    testEncryption(ONothingEncryption.NAME, "no");
  }

  public void testONothingEncryptedCompression() {
    testEncryption(ONothingEncryption.NAME, "T1JJRU5UREI=");
  }

  public void testCreatedNothingEncryptedDatabase() {
    OFileUtils.deleteRecursively(new File("target/" + DBNAME_DATABASETEST));

    final ODatabase db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_DATABASETEST);

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

      Orient.instance().getStorage(DBNAME_DATABASETEST).close(true, false);

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      Orient.instance().getStorage(DBNAME_DATABASETEST).close(true, false);

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
      try {
        db.open("admin", "admin");
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
        Orient.instance().getStorage(DBNAME_DATABASETEST).close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        Assert.fail();
      } catch (OSecurityException e) {
        Assert.assertTrue(true);
      } finally {
        db.activateOnCurrentThread();
        db.close();
        Orient.instance().getStorage(DBNAME_DATABASETEST).close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      db.activateOnCurrentThread();
      db.drop();
    }
  }

  public void testCreatedNothingEncryptedCluster() {
    OFileUtils.deleteRecursively(new File("target/" + DBNAME_CLUSTERTEST));

    final ODatabase db = new ODatabaseDocumentTx("plocal:target/" + DBNAME_CLUSTERTEST);

    db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();

    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("alter cluster TestEncryption encryption nothing")).execute();
      db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

      List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      db.open("admin", "admin");
      db.close();

      Orient.instance().getStorage(DBNAME_CLUSTERTEST).close(true, false);

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);
      db.close();

      Orient.instance().getStorage(DBNAME_CLUSTERTEST).close(true, false);

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");

      try {
        db.open("admin", "admin");
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

        result = db.query(new OSQLSynchQuery<ODocument>("select from OUser"));
        Assert.assertFalse(result.isEmpty());

      } finally {
        db.close();
        Orient.instance().getStorage(DBNAME_CLUSTERTEST).close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
      try {
        db.open("admin", "admin");
        db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      } finally {
        db.close();
        Orient.instance().getStorage(DBNAME_CLUSTERTEST).close(true, false);
      }

      db.setProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
      db.open("admin", "admin");
      result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      db.activateOnCurrentThread();
      db.drop();
    }
  }
}
