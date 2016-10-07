package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since 05.06.13
 */
public abstract class AbstractEncryptionTest {

  public void testEncryption(String name) {
    testEncryption(name, null);
  }

  public void testEncryption(String name, String options) {
    long seed = System.currentTimeMillis();
    System.out.println(name + " - Encryption seed " + seed);

    Random random = new Random(seed);
    final int iterationsCount = 1000;
    long encryptedSize = 0;
    for (int i = 0; i < iterationsCount; i++) {
      int contentSize = random.nextInt(10 * 1024 - 100) + 100;
      byte[] content = new byte[contentSize];
      random.nextBytes(content);

      final OEncryption en = OEncryptionFactory.INSTANCE.getEncryption(name, options);

      // FULL
      byte[] encryptedContent = en.encrypt(content);

      encryptedSize += encryptedContent.length;

      assertThat(content).isEqualTo(en.decrypt(encryptedContent));
      // PARTIAL (BUT FULL)
      encryptedContent = en.encrypt(content, 0, content.length);

      encryptedSize += encryptedContent.length;

      assertThat(content).isEqualTo(en.decrypt(encryptedContent));

      // REAL PARTIAL
      encryptedContent = en.encrypt(content, 1, content.length - 2);

      encryptedSize += encryptedContent.length - 2;

      assertThat(Arrays.copyOfRange(content, 1, content.length - 1)).isEqualTo(en.decrypt(encryptedContent));
    }

    System.out.println(
        "Encryption/Decryption test against " + name + " took: " + (System.currentTimeMillis() - seed) + "ms, total byte size: "
            + encryptedSize);
  }

  public void verifyDatabaseEncryption(ODatabase db) {
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

  public void verifyClusterEncryption(ODatabase db, String algorithm) {
    try {
      db.command(new OCommandSQL("create class TestEncryption")).execute();
      db.command(new OCommandSQL("alter cluster TestEncryption encryption " + algorithm)).execute();
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
