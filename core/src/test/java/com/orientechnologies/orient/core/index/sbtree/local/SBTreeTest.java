package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

/**
 * @author Andrey Lomakin
 * @since 12.08.13
 */
@Test
public class SBTreeTest {
  private static final int                  KEYS_COUNT = 500000;
  protected OSBTree<Integer, OIdentifiable> sbTree;
  private ODatabaseDocumentTx               databaseDocumentTx;
  private String                            buildDirectory;

  @BeforeClass
  public void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/localSBTreeTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    sbTree = new OSBTree<Integer, OIdentifiable>(".sbt", false, ".nbt", (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());
    sbTree.create("sbTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false);
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
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i), i + " key is absent");
      doReset();
    }

    Assert.assertEquals(0, (int) sbTree.firstKey());
    doReset();

    Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());
    doReset();

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(sbTree.get(i));
      doReset();
    }

  }

  protected void doReset() {
  }

  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<Integer>();
    final MersenneTwisterFast random = new MersenneTwisterFast();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      sbTree.put(key, new ORecordId(key % 32000, key));
      keys.add(key);

      doReset();

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    doReset();

    Assert.assertEquals(sbTree.lastKey(), keys.last());
    doReset();

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      doReset();
    }

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

      sbTree.put(key, new ORecordId(key % 32000, key));
      doReset();
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    doReset();

    Assert.assertEquals(sbTree.lastKey(), keys.last());
    doReset();

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      doReset();
    }

  }

  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<Integer>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
      keys.add(i);
    }

    Iterator<Integer> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      int key = keysIterator.next();
      if (key % 3 == 0) {
        sbTree.remove(key);
        doReset();
        keysIterator.remove();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    doReset();
    Assert.assertEquals(sbTree.lastKey(), keys.last());
    doReset();

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
        doReset();
      } else {
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
        doReset();
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

      sbTree.put(key, new ORecordId(key % 32000, key));
      doReset();
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      doReset();
    }

    Iterator<Integer> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      int key = keysIterator.next();

      if (key % 3 == 0) {
        sbTree.remove(key);
        keysIterator.remove();
        doReset();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    doReset();
    Assert.assertEquals(sbTree.lastKey(), keys.last());
    doReset();

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
        doReset();
      } else {
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
        doReset();
      }
    }
  }

  public void testKeyDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, i));
      doReset();
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    doReset();
    Assert.assertEquals((int) sbTree.lastKey(), (KEYS_COUNT - 1) % 3 == 0 ? KEYS_COUNT - 2 : KEYS_COUNT - 1);
    doReset();

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      doReset();
    }
  }

  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));

      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, i));

      if (i % 2 == 0)
        sbTree.put(KEYS_COUNT + i, new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));

      doReset();
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    doReset();

    Assert.assertEquals((int) sbTree.lastKey(), 2 * KEYS_COUNT - 2);
    doReset();

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));

      if (i % 2 == 0)
        Assert.assertEquals(sbTree.get(KEYS_COUNT + i), new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));

      doReset();
    }
  }

  public void testIterateEntriesMajor() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, key));
      keyValues.put(key, new ORecordId(key % 32000, key));
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  public void testIterateEntriesMinor() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, key));
      keyValues.put(key, new ORecordId(key % 32000, key));
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  public void testIterateEntriesBetween() {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<Integer, ORID>();
    MersenneTwisterFast random = new MersenneTwisterFast();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      sbTree.put(key, new ORecordId(key % 32000, key));
      keyValues.put(key, new ORecordId(key % 32000, key));
    }

    assertIterateBetweenEntries(keyValues, random, true, true, true);
    assertIterateBetweenEntries(keyValues, random, true, false, true);
    assertIterateBetweenEntries(keyValues, random, false, true, true);
    assertIterateBetweenEntries(keyValues, random, false, false, true);

    assertIterateBetweenEntries(keyValues, random, true, true, false);
    assertIterateBetweenEntries(keyValues, random, true, false, false);
    assertIterateBetweenEntries(keyValues, random, false, true, false);
    assertIterateBetweenEntries(keyValues, random, false, false, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    for (int i = 0; i < 5167; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 0; i < 3500; i++) {
      sbTree.remove(i);
      doReset();
    }

    Assert.assertEquals((int) sbTree.firstKey(), 3500);
    doReset();

    for (int i = 0; i < 3500; i++) {
      Assert.assertNull(sbTree.get(i));
      doReset();
    }

    for (int i = 3500; i < 5167; i++) {
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      doReset();
    }

  }

  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    for (int i = 0; i < 5167; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 5166; i > 1700; i--) {
      sbTree.remove(i);
      doReset();
    }

    Assert.assertEquals((int) sbTree.lastKey(), 1700);
    doReset();

    for (int i = 5166; i > 1700; i--) {
      Assert.assertNull(sbTree.get(i));
      doReset();
    }

    for (int i = 1700; i >= 0; i--) {
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      doReset();
    }

  }

  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    for (int i = 0; i < 12055; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
      doReset();
    }

    for (int i = 0; i < 1730; i++) {
      sbTree.remove(i);
      doReset();
    }

    for (int i = 3440; i < 6900; i++) {
      sbTree.remove(i);
      doReset();
    }

    for (int i = 8600; i < 12055; i++)
      sbTree.remove(i);

    Assert.assertEquals((int) sbTree.firstKey(), 1730);
    Assert.assertEquals((int) sbTree.lastKey(), 8599);

    Set<OIdentifiable> identifiables = new HashSet<OIdentifiable>();

    OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor = sbTree.iterateEntriesMinor(7200, true, true);
    cursorToSet(identifiables, cursor);

    for (int i = 7200; i >= 6900; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 3439; i >= 1730; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    cursor = sbTree.iterateEntriesMinor(7200, true, false);
    cursorToSet(identifiables, cursor);

    for (int i = 7200; i >= 6900; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 3439; i >= 1730; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    cursor = sbTree.iterateEntriesMajor(1740, true, true);
    cursorToSet(identifiables, cursor);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i < 8600; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    cursor = sbTree.iterateEntriesMajor(1740, true, false);
    cursorToSet(identifiables, cursor);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i < 8600; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    cursor = sbTree.iterateEntriesBetween(1740, true, 7200, true, true);
    cursorToSet(identifiables, cursor);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i <= 7200; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    cursor = sbTree.iterateEntriesBetween(1740, true, 7200, true, false);
    cursorToSet(identifiables, cursor);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i <= 7200; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());
  }

  public void testNullKeysInSBTree() {
    final OSBTree<Integer, OIdentifiable> nullSBTree = new OSBTree<Integer, OIdentifiable>(".sbt", false, ".nbt",
        (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());
    nullSBTree.create("nullSBTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, true);

    try {
      for (int i = 0; i < 10; i++)
        nullSBTree.put(i, new ORecordId(3, i));

      OIdentifiable identifiable = nullSBTree.get(null);
      Assert.assertNull(identifiable);

      nullSBTree.put(null, new ORecordId(10, 1000));

      identifiable = nullSBTree.get(null);
      Assert.assertEquals(identifiable, new ORecordId(10, 1000));

      OIdentifiable removed = nullSBTree.remove(5);
      Assert.assertEquals(removed, new ORecordId(3, 5));

      removed = nullSBTree.remove(null);
      Assert.assertEquals(removed, new ORecordId(10, 1000));

      removed = nullSBTree.remove(null);
      Assert.assertNull(removed);

      identifiable = nullSBTree.get(null);
      Assert.assertNull(identifiable);
    } finally {
      nullSBTree.delete();
    }
  }

  private void cursorToSet(Set<OIdentifiable> identifiables, OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor) {
    identifiables.clear();
    Map.Entry<Integer, OIdentifiable> entry = cursor.next(-1);
    while (entry != null) {
      identifiables.add(entry.getValue());
      entry = cursor.next(-1);
    }
  }

  private void assertIterateMajorEntries(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive,
      boolean ascSortOrder) {
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

      final OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor = sbTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<Integer, ORID>> iterator;
      if (ascSortOrder)
        iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
      else
        iterator = keyValues.descendingMap().subMap(keyValues.lastKey(), true, fromKey, keyInclusive).entrySet().iterator();

      while (iterator.hasNext()) {
        final Map.Entry<Integer, OIdentifiable> indexEntry = cursor.next(-1);
        final Map.Entry<Integer, ORID> entry = iterator.next();

        Assert.assertEquals(indexEntry.getKey(), entry.getKey());
        Assert.assertEquals(indexEntry.getValue(), entry.getValue());
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateMinorEntries(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random, boolean keyInclusive,
      boolean ascSortOrder) {
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

      final OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor = sbTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<Integer, ORID>> iterator;
      if (ascSortOrder)
        iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
      else
        iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<Integer, OIdentifiable> indexEntry = cursor.next(-1);
        Map.Entry<Integer, ORID> entry = iterator.next();

        Assert.assertEquals(indexEntry.getKey(), entry.getKey());
        Assert.assertEquals(indexEntry.getValue(), entry.getValue());
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateBetweenEntries(NavigableMap<Integer, ORID> keyValues, MersenneTwisterFast random,
      boolean fromInclusive, boolean toInclusive, boolean ascSortOrder) {
    long totalTime = 0;
    long totalIterations = 0;

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

      OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor = sbTree.iterateEntriesBetween(fromKey, fromInclusive, toKey,
          toInclusive, ascSortOrder);

      Iterator<Map.Entry<Integer, ORID>> iterator;
      if (ascSortOrder)
        iterator = keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
      else
        iterator = keyValues.descendingMap().subMap(toKey, toInclusive, fromKey, fromInclusive).entrySet().iterator();

      long startTime = System.currentTimeMillis();
      int iteration = 0;
      while (iterator.hasNext()) {
        iteration++;

        Map.Entry<Integer, OIdentifiable> indexEntry = cursor.next(-1);
        Assert.assertNotNull(indexEntry);

        Map.Entry<Integer, ORID> mapEntry = iterator.next();
        Assert.assertEquals(indexEntry.getKey(), mapEntry.getKey());
        Assert.assertEquals(indexEntry.getValue(), mapEntry.getValue());
      }

      long endTime = System.currentTimeMillis();

      totalIterations += iteration;
      totalTime += (endTime - startTime);

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }

    if (totalTime != 0)
      System.out.println("Iterations per second : " + (totalIterations * 1000) / totalTime);
  }

}
