package com.orientechnologies.orient.core.compression.impl;

import java.io.File;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author giastfader@github
 * @author Luca Garulli
 * @since 22.04.2015
 */
@Test
public class ODESCompressionTest extends AbstractCompressionTest {
  public void testODESEncryptedCompressionNoKey() {
    try {
      testCompression(ODESCompression.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testODESEncryptedCompressionInvalidKey() {
    try {
      testCompression(ODESCompression.NAME, "no");
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testODESEncryptedCompression() {
    testCompression(ODESCompression.NAME, "T1JJRU5UREI=");
  }

  public void testCreatedDESEncryptedDatabase() {
    OFileUtils.deleteRecursively(new File("target/testCreatedDESEncryptedDatabase"));

    final ODatabase db = new ODatabaseDocumentTx("plocal:target/testCreatedDESEncryptedDatabase");

    db.setProperty(OGlobalConfiguration.STORAGE_COMPRESSION_OPTIONS.getKey(), "T1JJRU5UREI=");

    db.create();

    db.command(new OCommandSQL("create class TestEncryption")).execute();
    db.command(new OCommandSQL("alter cluster TestEncryption compression des-encrypted")).execute();
    db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

    List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    db.close();

    db.open("admin", "admin");
    db.close();

    Orient.instance().getStorage("testCreatedDESEncryptedDatabase").close(true, false);

    db.setProperty(OGlobalConfiguration.STORAGE_COMPRESSION_OPTIONS.getKey(), "T1JJRU5UREI=");
    db.open("admin", "admin");
    result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    db.close();

    Orient.instance().getStorage("testCreatedDESEncryptedDatabase").close(true, false);

    db.setProperty(OGlobalConfiguration.STORAGE_COMPRESSION_OPTIONS.getKey(), "invalidPassword");
    try {
      db.open("admin", "admin");
      db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.fail();
    } catch (ODatabaseException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityException);
    } finally {
      db.close();
      Orient.instance().getStorage("testCreatedDESEncryptedDatabase").close(true, false);
    }

    db.setProperty(OGlobalConfiguration.STORAGE_COMPRESSION_OPTIONS.getKey(), "T1JJRU5UREI-");
    try {
      db.open("admin", "admin");
      db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
      Assert.fail();
    } catch (OStorageException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityException);
    } finally {
      db.close();
      Orient.instance().getStorage("testCreatedDESEncryptedDatabase").close(true, false);
    }

    db.setProperty(OGlobalConfiguration.STORAGE_COMPRESSION_OPTIONS.getKey(), "T1JJRU5UREI=");
    db.open("admin", "admin");
    result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    db.close();
  }
}
