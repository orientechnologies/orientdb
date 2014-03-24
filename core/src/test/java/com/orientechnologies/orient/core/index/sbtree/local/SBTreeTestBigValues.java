package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/1/13
 */
@Test
public class SBTreeTestBigValues {
  private static final int           KEYS_COUNT = 60000;

  private ODatabaseDocumentTx        databaseDocumentTx;

  protected OSBTree<Integer, byte[]> sbTree;
  private String                     buildDirectory;

  @BeforeClass
  public void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/localSBTreeBigValuesTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    sbTree = new OSBTree<Integer, byte[]>(".sbt", 1, false);
    sbTree.create("sbTree", OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null,
        (OStorageLocalAbstract) databaseDocumentTx.getStorage().getUnderlying());
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    sbTree.clear();
  }

  @AfterClass
  public void afterClass() throws Exception {
    sbTree.clear();
    sbTree.delete();

    databaseDocumentTx.drop();
  }

  public void testPut() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + i));
    }

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertEquals(sbTree.get(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + i), i + " key is absent");

    Assert.assertEquals(0, (int) sbTree.firstKey());
    Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++)
      Assert.assertNull(sbTree.get(i));

  }

  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<Integer>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomUniform seed : " + seed);

    final MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      sbTree.put(key, createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      keys.add(key);
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys)
      Assert.assertEquals(sbTree.get(key), createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
  }

  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<Integer>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      sbTree.put(key, createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      keys.add(key);
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys)
      Assert.assertEquals(sbTree.get(key), createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
  }

  public void testKeyDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals((int) sbTree.lastKey(), (KEYS_COUNT - 1) % 3 == 0 ? KEYS_COUNT - 2 : KEYS_COUNT - 1);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }
  }

  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      keys.add(i);
    }

    Iterator<Integer> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      int key = keysIterator.next();
      if (key % 3 == 0) {
        sbTree.remove(key);
        keysIterator.remove();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
      } else {
        Assert.assertEquals(sbTree.get(key), createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      }
    }
  }

  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<Integer>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      sbTree.put(key, createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    Iterator<Integer> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      int key = keysIterator.next();

      if (key % 3 == 0) {
        sbTree.remove(key);
        keysIterator.remove();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
      } else {
        Assert.assertEquals(sbTree.get(key), createValue(key, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      }
    }
  }

  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));

      Assert.assertEquals(sbTree.get(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));

      if (i % 2 == 0)
        sbTree.put(KEYS_COUNT + i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals((int) sbTree.lastKey(), 2 * KEYS_COUNT - 2);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));

      if (i % 2 == 0)
        Assert.assertEquals(sbTree.get(KEYS_COUNT + i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }
  }

  public void testKeysUpdateFromSmallToBig() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 2 == 0)
        sbTree.put(i, createValue(i, 1024));
      else
        sbTree.put(i / 2, createValue(i / 2, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }

    for (int i = 0; i < KEYS_COUNT / 2; i++) {
      Assert.assertEquals(sbTree.get(i), createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
    }
  }

  public void testKeysUpdateFromBigToSmall() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 2 == 0)
        sbTree.put(i, createValue(i, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE + 4));
      else
        sbTree.put(i / 2, createValue(i / 2, 1024));
    }

    for (int i = 0; i < KEYS_COUNT / 2; i++) {
      Assert.assertEquals(sbTree.get(i), createValue(i, 1024));
    }
  }

  public void testKeysUpdateFromSmallToSmall() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 2 == 0)
        sbTree.put(i, createValue(i, 512));
      else
        sbTree.put(i / 2, createValue(i / 2, 1024));
    }

    for (int i = 0; i < KEYS_COUNT / 2; i++) {
      Assert.assertEquals(sbTree.get(i), createValue(i, 1024));
    }
  }

  private byte[] createValue(int value, int byteArraySize) {
    byte[] binaryValue = new byte[byteArraySize];
    byte[] intValue = new byte[4];
    OIntegerSerializer.INSTANCE.serialize(value, intValue, 0);

    int offset = 0;
    while (offset < byteArraySize) {
      System.arraycopy(intValue, 0, binaryValue, offset, Math.min(byteArraySize - offset, intValue.length));
      offset += intValue.length;
    }

    return binaryValue;
  }
}
