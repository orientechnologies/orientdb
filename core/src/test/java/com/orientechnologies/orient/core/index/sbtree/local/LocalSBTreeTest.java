package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 12.08.13
 */
@Test
public class LocalSBTreeTest {
  private static final int      KEYS_COUNT = 500000;

  private ODatabaseDocumentTx   databaseDocumentTx;

  private OLocalSBTree<Integer> localSBTree;
  private String                buildDirectory;

  @BeforeClass
  public void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localSBTreeTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localSBTree = new OLocalSBTree<Integer>(".sbt");
    localSBTree.create("localSBTree", OIntegerSerializer.INSTANCE, (OStorageLocal) databaseDocumentTx.getStorage());
  }

  @AfterMethod
  public void afterMethod() {
    localSBTree.clear();
  }

  @AfterClass
  public void afterClass() throws Exception {
    localSBTree.clear();
    localSBTree.delete();
    databaseDocumentTx.drop();
  }

  public void testKeyPut() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)), i
          + " key is absent");

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++)
      Assert.assertNull(localSBTree.get(i));

  }

  public void testKeyPutRandomUniform() {
    final Set<Integer> keys = new HashSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys)
      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  public void testKeyPutRandomGaussian() {
    Set<Integer> keys = new HashSet<Integer>();
    long seed = 1376477211861L;

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys)
      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  public void testKeyDeleteRandomUniform() {
    HashSet<Integer> keys = new HashSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      keys.add(i);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localSBTree.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localSBTree.get(key));
      } else {
        Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDeleteRandomGaussian() {
    HashSet<Integer> keys = new HashSet<Integer>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localSBTree.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localSBTree.get(key));
      } else {
        Assert.assertEquals(localSBTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localSBTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localSBTree.get(i));
      else
        Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }
  }

  public void testKeyAddDelete() {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localSBTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localSBTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        localSBTree.put(KEYS_COUNT + i,
            new ORecordId((KEYS_COUNT + i) % 32000, OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localSBTree.get(i));
      else
        Assert.assertEquals(localSBTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        Assert.assertEquals(localSBTree.get(KEYS_COUNT + i), new ORecordId((KEYS_COUNT + i) % 32000,
            OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }
  }

  public void testValuesMajor() {
    TreeMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertMajorValues(keyValues, random, true);
    assertMajorValues(keyValues, random, false);
  }

  public void testValuesMinor() {
    TreeMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertMinorValues(keyValues, random, true);
    assertMinorValues(keyValues, random, false);
  }

  public void testValuesBetween() {
    TreeMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      localSBTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertBetweenValues(keyValues, random, true, true);
    assertBetweenValues(keyValues, random, true, false);
    assertBetweenValues(keyValues, random, false, true);
    assertBetweenValues(keyValues, random, false, false);
  }

  private void assertMajorValues(TreeMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0)
        fromKey = random.nextInt(upperBorder);
      else
        fromKey = random.nextInt(Integer.MAX_VALUE);

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null)
          fromKey = includedKey;
        else
          fromKey = keyValues.floorKey(fromKey);
      }

      int maxValuesToFetch = 10000;
      Collection<ORID> orids = localSBTree.getValuesMajor(fromKey, keyInclusive, maxValuesToFetch);

      Set<ORID> result = new HashSet<ORID>(orids);

      Iterator<ORID> valuesIterator = keyValues.tailMap(fromKey, keyInclusive).values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        ORID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext())
        Assert.assertEquals(fetchedValues, maxValuesToFetch);

      Assert.assertEquals(result.size(), 0);
    }
  }

  private void assertMinorValues(TreeMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int toKey;
      if (upperBorder > 0)
        toKey = random.nextInt(upperBorder) - 5000;
      else
        toKey = random.nextInt(Integer.MAX_VALUE) - 5000;

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null)
          toKey = includedKey;
        else
          toKey = keyValues.floorKey(toKey);
      }

      int maxValuesToFetch = 10000;
      Collection<ORID> orids = localSBTree.getValuesMinor(toKey, keyInclusive, maxValuesToFetch);

      Set<ORID> result = new HashSet<ORID>(orids);

      Iterator<ORID> valuesIterator = keyValues.headMap(toKey, keyInclusive).descendingMap().values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        ORID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext())
        Assert.assertEquals(fetchedValues, maxValuesToFetch);

      Assert.assertEquals(result.size(), 0);
    }
  }

  private void assertBetweenValues(TreeMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean fromInclusive,
      boolean toInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0)
        fromKey = random.nextInt(upperBorder);
      else
        fromKey = random.nextInt(Integer.MAX_VALUE - 1);

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null)
          fromKey = includedKey;
        else
          fromKey = keyValues.floorKey(fromKey);
      }

      int toKey = random.nextInt() + fromKey + 1;
      if (toKey < 0)
        toKey = Integer.MAX_VALUE;

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null)
          toKey = includedKey;
        else
          toKey = keyValues.floorKey(toKey);
      }

      if (fromKey > toKey)
        toKey = fromKey;

      int maxValuesToFetch = 10000;

      Collection<ORID> orids = localSBTree.getValuesBetween(fromKey, fromInclusive, toKey, toInclusive, maxValuesToFetch);
      Set<ORID> result = new HashSet<ORID>(orids);

      Iterator<ORID> valuesIterator = keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        ORID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext())
        Assert.assertEquals(fetchedValues, maxValuesToFetch);

      Assert.assertEquals(result.size(), 0);
    }
  }
}
