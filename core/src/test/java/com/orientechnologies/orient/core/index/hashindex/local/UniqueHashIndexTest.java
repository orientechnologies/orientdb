package com.orientechnologies.orient.core.index.hashindex.local;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Andrey Lomakin
 * @since 19.02.13
 */
@Test
public class UniqueHashIndexTest {
  private static final int    KEYS_COUNT = 200000;

  private ODatabaseDocumentTx databaseDocumentTx;
  private OUniqueHashIndex    hashIndex  = new OUniqueHashIndex();

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    hashIndex.create("uhashIndexTest", new OSimpleKeyIndexDefinition(OType.INTEGER), databaseDocumentTx,
        OMetadata.CLUSTER_INDEX_NAME, new int[0], null);
  }

  @AfterClass
  public void afterClass() {
    hashIndex.close();
    databaseDocumentTx.drop();
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
    hashIndex.clear();
  }

  public void testKeyPut() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final ORID rid = new ORecordId(0, new OClusterPositionLong(i));

      hashIndex.put(i, rid);
      Assert.assertEquals(hashIndex.get(i), rid);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      final ORID rid = new ORecordId(0, new OClusterPositionLong(i));

      Assert.assertEquals(hashIndex.get(i), rid, i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertFalse(hashIndex.contains(i));
    }
  }

  public void testKeyDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      hashIndex.put(i, new ORecordId(0, new OClusterPositionLong(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertTrue(hashIndex.remove(i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertFalse(hashIndex.contains(i));
      else
        Assert.assertTrue(hashIndex.contains(i));
    }
  }

  public void testKeyAddDelete() {
    for (int i = 0; i < KEYS_COUNT; i++)
      hashIndex.put(i, new ORecordId(0, new OClusterPositionLong(i)));

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertTrue(hashIndex.remove(i));

      if (i % 2 == 0)
        hashIndex.put(KEYS_COUNT + i, new ORecordId(0, new OClusterPositionLong(KEYS_COUNT + i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertFalse(hashIndex.contains(i));
      else
        Assert.assertTrue(hashIndex.contains(i));

      if (i % 2 == 0)
        Assert.assertTrue(hashIndex.contains(KEYS_COUNT + i), "i " + (KEYS_COUNT + i));

    }
  }

}
