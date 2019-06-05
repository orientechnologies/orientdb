package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.08.13
 */
public class SBTreeTestIT {
  private static final int KEYS_COUNT = 500000;
  OSBTreeV1<Integer, OIdentifiable> sbTree;
  protected ODatabaseSession databaseDocumentTx;
  protected String           buildDirectory;
  protected OrientDB         orientDB;

  String dbName;

  @Before
  public void before() throws Exception {
    buildDirectory = System.getProperty("buildDirectory", ".") + File.separator + SBTreeTestIT.class.getSimpleName();

    dbName = "localSBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    sbTree = new OSBTreeV1<>("sbTree", ".sbt", ".nbt",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false, null);
  }

  @After
  public void afterMethod() throws Exception {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(i + " key is absent", sbTree.get(i), new ORecordId(i % 32000, i));
    }

    Assert.assertEquals(0, (int) sbTree.firstKey());

    Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(sbTree.get(i));
    }

  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<>();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      sbTree.put(key, new ORecordId(key % 32000, key));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      sbTree.put(key, new ORecordId(key % 32000, key));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }

  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
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
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (key < 0)
        continue;

      sbTree.put(key, new ORecordId(key % 32000, key));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
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
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, i));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    //noinspection ConstantConditions
    Assert.assertEquals((int) sbTree.lastKey(), (KEYS_COUNT - 1) % 3 == 0 ? KEYS_COUNT - 2 : KEYS_COUNT - 1);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));

      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(sbTree.remove(i), new ORecordId(i % 32000, i));

      if (i % 2 == 0)
        sbTree.put(KEYS_COUNT + i, new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));

    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);

    Assert.assertEquals((int) sbTree.lastKey(), 2 * KEYS_COUNT - 2);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(sbTree.get(i));
      else
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));

      if (i % 2 == 0)
        Assert.assertEquals(sbTree.get(KEYS_COUNT + i), new ORecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));
    }
  }

  @Test
  public void testIterateEntriesMajor() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();
    Random random = new Random();

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

  @Test
  public void testIterateEntriesMinor() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();
    Random random = new Random();

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

  @Test
  public void testIterateEntriesBetween() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();
    Random random = new Random();

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

  @Test
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    for (int i = 0; i < 5167; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < 3500; i++) {
      sbTree.remove(i);
    }

    Assert.assertEquals((int) sbTree.firstKey(), 3500);
    for (int i = 0; i < 3500; i++) {
      Assert.assertNull(sbTree.get(i));
    }

    for (int i = 3500; i < 5167; i++) {
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
    }

  }

  @Test
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    for (int i = 0; i < 5167; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
    }

    for (int i = 5166; i > 1700; i--) {
      sbTree.remove(i);
    }

    Assert.assertEquals((int) sbTree.lastKey(), 1700);

    for (int i = 5166; i > 1700; i--) {
      Assert.assertNull(sbTree.get(i));
    }

    for (int i = 1700; i >= 0; i--) {
      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
    }

  }

  @Test
  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    for (int i = 0; i < 12055; i++) {
      sbTree.put(i, new ORecordId(i % 32000, i));
    }

    for (int i = 0; i < 1730; i++) {
      sbTree.remove(i);
    }

    for (int i = 3440; i < 6900; i++) {
      sbTree.remove(i);
    }

    for (int i = 8600; i < 12055; i++)
      sbTree.remove(i);

    Assert.assertEquals((int) sbTree.firstKey(), 1730);
    Assert.assertEquals((int) sbTree.lastKey(), 8599);

    Set<OIdentifiable> identifiables = new HashSet<>();

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

  @Test
  public void testNullKeysInSBTree() throws Exception {
    final OSBTreeV1<Integer, OIdentifiable> nullSBTree = new OSBTreeV1<>("nullSBTree", ".sbt", ".nbt",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
    nullSBTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, true, null);

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

  private void assertIterateMajorEntries(NavigableMap<Integer, ORID> keyValues, Random random, boolean keyInclusive,
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

  private void assertIterateMinorEntries(NavigableMap<Integer, ORID> keyValues, Random random, boolean keyInclusive,
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

  private void assertIterateBetweenEntries(NavigableMap<Integer, ORID> keyValues, Random random, boolean fromInclusive,
      boolean toInclusive, boolean ascSortOrder) {
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

      OSBTree.OSBTreeCursor<Integer, OIdentifiable> cursor = sbTree
          .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder);

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
