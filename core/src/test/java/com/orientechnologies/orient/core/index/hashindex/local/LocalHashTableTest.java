package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 19.02.13
 */
@Test
public class LocalHashTableTest {
  private static final int                 KEYS_COUNT = 1600000;

  private ODatabaseDocumentTx              databaseDocumentTx;

  private OLocalHashTable<Integer, String> localHashTable;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localHashTableTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTable<Integer, String>(".imc", ".tsc", ".obf", murmurHash3HashFunction);

    localHashTable.create("localHashTableTest", OIntegerSerializer.INSTANCE, OStringSerializer.INSTANCE, null,
        (OStorageLocal) databaseDocumentTx.getStorage());
  }

  @AfterClass
  public void afterClass() throws Exception {
    localHashTable.clear();
    localHashTable.delete();
    databaseDocumentTx.drop();
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
    localHashTable.clear();
  }

  public void testKeyPut() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
      Assert.assertEquals(localHashTable.get(i), i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(localHashTable.get(i), i + "", i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(localHashTable.get(i));
    }
  }

  public void testKeyPutRandomUniform() {
    final Set<Integer> keys = new HashSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  public void testKeyPutRandomGaussian() {
    Set<Integer> keys = new HashSet<Integer>();
    MersenneTwisterFast random = new MersenneTwisterFast();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  public void testKeyDeleteRandomUniform() {
    HashSet<Integer> keys = new HashSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
      keys.add(i);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  public void testKeyDeleteRandomGaussian() {
    HashSet<Integer> keys = new HashSet<Integer>();

    MersenneTwisterFast random = new MersenneTwisterFast();
    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localHashTable.put(key, key + "");
      keys.add(key);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  public void testKeyDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), "" + i);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");
    }
  }

  public void testKeyAddDelete() {
    for (int i = 0; i < KEYS_COUNT; i++)
      localHashTable.put(i, i + "");

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), i + "");

      if (i % 2 == 0)
        localHashTable.put(KEYS_COUNT + i, (KEYS_COUNT + i) + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");

      if (i % 2 == 0)
        Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), "" + (KEYS_COUNT + i));
    }
  }
}
