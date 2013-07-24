package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QDiskCache;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 13.03.13
 */
@Test
public class LocalHashTableIterationTest {
  private static final int                 KEYS_COUNT = 1600000;

  private ODatabaseDocumentTx              databaseDocumentTx;

  private OLocalHashTable<Integer, String> localHashTable;
  private O2QDiskCache                     buffer;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localHashTableIterationTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    buffer = new O2QDiskCache(400 * 1024 * 1024, 15000, ODirectMemoryFactory.INSTANCE.directMemory(), null,
        OHashIndexBucket.MAX_BUCKET_SIZE_BYTES, (OStorageLocal) databaseDocumentTx.getStorage(), false);

    OHashFunction<Integer> hashFunction = new OHashFunction<Integer>() {
      @Override
      public long hashCode(Integer value) {
        return Long.MAX_VALUE / 2 + value;
      }
    };

    localHashTable = new OLocalHashTable<Integer, String>(".imc", ".tsc", ".obf", hashFunction);

    localHashTable.create("localHashTableIterationTest", OIntegerSerializer.INSTANCE, OStringSerializer.INSTANCE,
        (OStorageLocal) databaseDocumentTx.getStorage());
  }

  @AfterClass
  public void afterClass() throws Exception {
    localHashTable.clear();
    localHashTable.delete();
    buffer.clear();
    buffer.close();
    databaseDocumentTx.drop();
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
    localHashTable.clear();
  }

  public void testNextHaveRightOrder() throws Exception {
    SortedSet<Integer> keys = new TreeSet<Integer>();
    keys.clear();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), "" + key);
      }
    }

    OHashIndexBucket.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(Integer.MIN_VALUE);
    int curPos = 0;
    for (int key : keys) {
      int sKey = entries[curPos].key;

      Assert.assertEquals(key, sKey, "" + key);
      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  public void testNextSkipsRecordValid() throws Exception {
    List<Integer> keys = new ArrayList<Integer>();
    keys.clear();

    final MersenneTwisterFast random = new MersenneTwisterFast();
    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), "" + key);
      }
    }

    Collections.sort(keys);

    OHashIndexBucket.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(keys.get(10));
    int curPos = 0;
    for (int key : keys) {
      if (key < keys.get(10)) {
        continue;
      }
      int sKey = entries[curPos].key;
      Assert.assertEquals(key, sKey);

      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  @Test(enabled = false)
  public void testNextHaveRightOrderUsingNextMethod() throws Exception {
    List<Integer> keys = new ArrayList<Integer>();
    keys.clear();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        localHashTable.put(key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }

    Collections.sort(keys);

    for (int key : keys) {
      OHashIndexBucket.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(key);
      Assert.assertTrue(key == entries[0].key);
    }

    for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
      int key = keys.get(j);
      int sKey = localHashTable.higherEntries(key)[0].key;
      Assert.assertTrue(sKey == keys.get(j + 1));
    }
  }

}
