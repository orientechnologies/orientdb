package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.util.*;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 12.08.13
 */
@Test
public class OSBTreeBonsaiTest {
  private static final int                        KEYS_COUNT = 500000;

  private ODatabaseDocumentTx                     databaseDocumentTx;

  protected OSBTreeBonsai<Integer, OIdentifiable> sbTree;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = "./target";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/localSBTreeBonsaiTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    sbTree = new OSBTreeBonsai<Integer, OIdentifiable>(".irs", false);
    sbTree.create("OSBTreeBonsaiTest", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE,
        (OStorageLocalAbstract) databaseDocumentTx.getStorage());
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

  public void testKeyPut() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)), i
          + " key is absent");

    Assert.assertEquals(0, (int) sbTree.firstKey());
    Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++)
      Assert.assertNull(sbTree.get(i));

  }

  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys)
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  public void testKeyPutRandomGaussian() throws Exception {
    final double mx = Integer.MAX_VALUE / 2.;
    final double dx = Integer.MAX_VALUE / 8.;

    NavigableSet<Integer> keys = new TreeSet<Integer>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = generateGaussianKey(mx, dx, random);

      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys)
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
  }

  private int generateGaussianKey(double mx, double dx, MersenneTwisterFast random) {
    double v;
    do {
      v = random.nextGaussian() * dx + mx;
    } while (v < 0 || v > Integer.MAX_VALUE);
    return (int) v;
  }

  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
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
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDeleteRandomGaussian() throws Exception {
    final double mx = Integer.MAX_VALUE / 2.;
    final double dx = Integer.MAX_VALUE / 8.;

    NavigableSet<Integer> keys = new TreeSet<Integer>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    MersenneTwisterFast random = new MersenneTwisterFast(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = generateGaussianKey(mx, dx, random);

      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
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
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      }
    }
  }

  public void testKeyDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals((int) sbTree.lastKey(), (KEYS_COUNT - 1) % 3 == 0 ? KEYS_COUNT - 2 : KEYS_COUNT - 1);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }
  }

  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        sbTree.put(KEYS_COUNT + i,
            new ORecordId((KEYS_COUNT + i) % 32000, OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals((int) sbTree.lastKey(), 2 * KEYS_COUNT - 2);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

      if (i % 2 == 0)
        Assert.assertEquals(sbTree.get(KEYS_COUNT + i),
            new ORecordId((KEYS_COUNT + i) % 32000, OClusterPositionFactory.INSTANCE.valueOf(KEYS_COUNT + i)));
    }
  }

  public void testValuesMajor() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertMajorValues(keyValues, random, true);
    assertMajorValues(keyValues, random, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  public void testValuesMinor() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertMinorValues(keyValues, random, true);
    assertMinorValues(keyValues, random, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  public void testValuesBetween() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
      keyValues.put(key, new ORecordId(key % 32000, OClusterPositionFactory.INSTANCE.valueOf(key)));
    }

    assertBetweenValues(keyValues, random, true, true);
    assertBetweenValues(keyValues, random, true, false);
    assertBetweenValues(keyValues, random, false, true);
    assertBetweenValues(keyValues, random, false, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  private void assertMajorValues(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive) {
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
      Collection<OIdentifiable> orids = sbTree.getValuesMajor(fromKey, keyInclusive, maxValuesToFetch);

      Set<OIdentifiable> result = new HashSet<OIdentifiable>(orids);

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

  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    for (int i = 0; i < 110; i++)
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

    for (int i = 0; i < 56; i++)
      sbTree.remove(i);

    Assert.assertEquals((int) sbTree.firstKey(), 56);

    for (int i = 0; i < 56; i++)
      Assert.assertNull(sbTree.get(i));

    for (int i = 56; i < 110; i++)
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
  }

  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    for (int i = 0; i < 110; i++)
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

    for (int i = 110; i > 50; i--)
      sbTree.remove(i);

    Assert.assertEquals((int) sbTree.lastKey(), 50);

    for (int i = 110; i > 50; i--)
      Assert.assertNull(sbTree.get(i));

    for (int i = 50; i >= 0; i--)
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
  }

  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    for (int i = 0; i < 326; i++)
      sbTree.put(i, new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));

    for (int i = 0; i < 60; i++)
      sbTree.remove(i);

    for (int i = 100; i < 220; i++)
      sbTree.remove(i);

    for (int i = 260; i < 326; i++)
      sbTree.remove(i);

    Assert.assertEquals((int) sbTree.firstKey(), 60);
    Assert.assertEquals((int) sbTree.lastKey(), 259);

    Collection<OIdentifiable> result = sbTree.getValuesMinor(250, true, -1);

    Set<OIdentifiable> identifiables = new HashSet<OIdentifiable>(result);
    for (int i = 250; i >= 220; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    for (int i = 99; i >= 60; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    result = sbTree.getValuesMajor(70, true, -1);
    identifiables = new HashSet<OIdentifiable>(result);

    for (int i = 70; i < 100; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    for (int i = 220; i < 260; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    result = sbTree.getValuesBetween(70, true, 250, true, -1);
    identifiables = new HashSet<OIdentifiable>(result);

    for (int i = 70; i < 100; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    for (int i = 220; i <= 250; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());
  }

  private void assertMinorValues(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive) {
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
      Collection<OIdentifiable> orids = sbTree.getValuesMinor(toKey, keyInclusive, maxValuesToFetch);

      Set<OIdentifiable> result = new HashSet<OIdentifiable>(orids);

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

  private void assertBetweenValues(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean fromInclusive,
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

      Collection<OIdentifiable> orids = sbTree.getValuesBetween(fromKey, fromInclusive, toKey, toInclusive, maxValuesToFetch);
      Set<OIdentifiable> result = new HashSet<OIdentifiable>(orids);

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
