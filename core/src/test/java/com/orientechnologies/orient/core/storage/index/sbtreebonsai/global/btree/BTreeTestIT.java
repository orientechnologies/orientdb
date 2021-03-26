package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BTreeTestIT {

  public static final String DB_NAME = "bTreeTest";
  public static final String DIR_NAME = "/globalBTreeTest";
  private static OrientDB orientDB;
  private static BTree bTree;
  private static OAtomicOperationsManager atomicOperationsManager;
  private static OAbstractPaginatedStorage storage;
  private static String buildDirectory;

  @Parameterized.Parameters
  public static Iterable<Integer> keysCount() {
    return IntStream.range(0, 21).map(val -> 1 << val).boxed().collect(Collectors.toList());
  }

  private final int keysCount;

  public BTreeTestIT(int keysCount) {
    this.keysCount = keysCount;
  }

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target" + DIR_NAME;
    } else {
      buildDirectory += DIR_NAME;
    }

    OFileUtils.deleteRecursively(new File(buildDirectory));

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    if (orientDB.exists(DB_NAME)) {
      orientDB.drop(DB_NAME);
    }

    orientDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession databaseSession = orientDB.open(DB_NAME, "admin", "admin");
    storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseSession).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    databaseSession.close();
  }

  @AfterClass
  public static void afterClass() {
    orientDB.drop(DB_NAME);
    orientDB.close();

    OFileUtils.deleteRecursively(new File(buildDirectory));
  }

  @Before
  public void beforeMethod() throws Exception {

    bTree = new BTree(storage, "bTree", ".sbc");
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> bTree.create(atomicOperation));
  }

  @After
  public void afterMethod() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> bTree.delete(atomicOperation));
  }

  @Test
  public void testKeyPut() throws Exception {
    EdgeKey firstKey = null;
    EdgeKey lastKey = null;
    long start = System.nanoTime();
    for (int i = 0; i < keysCount; i++) {
      final int index = i;
      final EdgeKey key = new EdgeKey(42, index % 32000, index);
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> bTree.put(atomicOperation, key, index + 1));

      if (firstKey == null) {
        firstKey = key;
        lastKey = key;
      } else {
        if (key.compareTo(lastKey) > 0) {
          lastKey = key;
        }
        if (key.compareTo(firstKey) < 0) {
          firstKey = key;
        }
      }
    }
    long end = System.nanoTime();
    System.out.printf("%d us per insert%n", (end - start) / 1_000 / keysCount);

    start = System.nanoTime();
    for (int i = 0; i < keysCount; i++) {
      Assertions.assertThat(bTree.get(new EdgeKey(42, i % 32000, i))).isEqualTo(i + 1);
    }
    end = System.nanoTime();

    System.out.printf("%d us per get%n", (end - start) / 1_000 / keysCount);

    Assert.assertEquals(firstKey, bTree.firstKey());
    Assert.assertEquals(lastKey, bTree.lastKey());

    for (int i = keysCount; i < keysCount + 100; i++) {
      Assert.assertEquals(bTree.get(new EdgeKey(42, i % 32000, i)), -1);
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<EdgeKey> keys = new TreeSet<>();
    final Random random = new Random();

    while (keys.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            int val = random.nextInt(Integer.MAX_VALUE);
            final EdgeKey key = new EdgeKey(42, val, val);
            bTree.put(atomicOperation, key, val);

            keys.add(key);
            Assert.assertEquals(bTree.get(key), val);
          });
    }

    Assert.assertEquals(bTree.firstKey(), keys.first());
    Assert.assertEquals(bTree.lastKey(), keys.last());

    for (EdgeKey key : keys) {
      Assert.assertEquals(bTree.get(key), key.targetPosition);
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<EdgeKey> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();
    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            int val;
            do {
              val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
            } while (val < 0);

            final EdgeKey key = new EdgeKey(42, val, val);
            bTree.put(atomicOperation, key, val);

            keys.add(key);
            Assert.assertEquals(bTree.get(key), val);
          });
    }

    Assert.assertEquals(bTree.firstKey(), keys.first());
    Assert.assertEquals(bTree.lastKey(), keys.last());

    for (EdgeKey key : keys) {
      Assert.assertEquals(bTree.get(key), key.targetPosition);
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<EdgeKey> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      final int val = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> bTree.put(atomicOperation, key, val));
      keys.add(key);
    }

    Iterator<EdgeKey> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      EdgeKey key = keysIterator.next();
      if (key.targetPosition % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> bTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    if (!keys.isEmpty()) {
      Assert.assertEquals(bTree.firstKey(), keys.first());
      Assert.assertEquals(bTree.lastKey(), keys.last());
    } else {
      Assert.assertNull(bTree.firstKey());
      Assert.assertNull(bTree.lastKey());
    }

    for (final EdgeKey key : keys) {
      if (key.targetPosition % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(key.targetPosition, bTree.get(key));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<EdgeKey> keys = new TreeSet<>();

    long seed = System.currentTimeMillis();
    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      final int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }

      EdgeKey key = new EdgeKey(42, val, val);
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> bTree.put(atomicOperation, key, val));
      keys.add(key);

      Assert.assertEquals(bTree.get(key), val);
    }

    Iterator<EdgeKey> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      EdgeKey key = keysIterator.next();

      if (key.targetPosition % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> bTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    if (!keys.isEmpty()) {
      Assert.assertEquals(bTree.firstKey(), keys.first());
      Assert.assertEquals(bTree.lastKey(), keys.last());
    } else {
      Assert.assertNull(bTree.firstKey());
      Assert.assertNull(bTree.lastKey());
    }

    for (EdgeKey key : keys) {
      if (key.targetPosition % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(bTree.get(key), key.targetPosition);
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    System.out.println("Keys count " + keysCount);

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      final int val = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> bTree.put(atomicOperation, key, val));
    }

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      if (key.targetPosition % 3 == 0) {

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(bTree.remove(atomicOperation, key), key.targetPosition));
      }
    }

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      if (i % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(i, bTree.get(key));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    System.out.println("Keys count " + keysCount);

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> bTree.put(atomicOperation, key, key.targetCluster % 5));

      Assert.assertEquals(bTree.get(key), key.targetCluster % 5);
    }

    for (int i = 0; i < keysCount; i++) {
      final int index = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            if (index % 3 == 0) {
              final EdgeKey key = new EdgeKey(42, index, index);
              Assert.assertEquals(bTree.remove(atomicOperation, key), key.targetCluster % 5);
            }

            if (index % 2 == 0) {
              final EdgeKey key = new EdgeKey(42, index + keysCount, index + keysCount);
              bTree.put(atomicOperation, key, (index + keysCount) % 5);
            }
          });
    }

    for (int i = 0; i < keysCount; i++) {
      {
        final EdgeKey key = new EdgeKey(42, i, i);
        if (i % 3 == 0) {
          Assert.assertEquals(-1, bTree.get(key));
        } else {
          Assert.assertEquals(i % 5, bTree.get(key));
        }
      }

      if (i % 2 == 0) {
        final EdgeKey key = new EdgeKey(42, i + keysCount, i + keysCount);
        Assert.assertEquals(bTree.get(key), (i + keysCount) % 5);
      }
    }
  }

  @Test
  public void testIterateEntriesMajor() throws Exception {
    System.out.printf("Keys count %d%n", keysCount);

    NavigableMap<EdgeKey, Integer> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMajor: " + seed);
    final Random random = new Random(seed);

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final int val = random.nextInt(Integer.MAX_VALUE);
            final EdgeKey key = new EdgeKey(42, val, val % 64937);

            bTree.put(atomicOperation, key, val);
            keyValues.put(key, val);
          });

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(bTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(bTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(
      NavigableMap<EdgeKey, Integer> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    EdgeKey[] keys = new EdgeKey[keyValues.size()];
    int index = 0;

    for (EdgeKey key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      final int fromKeyIndex = random.nextInt(keys.length);
      EdgeKey fromKey = keys[fromKeyIndex];

      if (random.nextBoolean() && fromKey.targetPosition > Long.MIN_VALUE) {
        fromKey = new EdgeKey(fromKey.ridBagId, fromKey.targetCluster, fromKey.targetPosition - 1);
      }

      final Iterator<ORawPair<EdgeKey, Integer>> indexIterator;
      try (Stream<ORawPair<EdgeKey, Integer>> stream =
          bTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<EdgeKey, Integer>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
        } else {
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(keyValues.lastKey(), true, fromKey, keyInclusive)
                  .entrySet()
                  .iterator();
        }

        while (iterator.hasNext()) {
          final ORawPair<EdgeKey, Integer> indexEntry = indexIterator.next();
          final Map.Entry<EdgeKey, Integer> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  @Test
  public void testIterateEntriesMinor() throws Exception {
    System.out.printf("Keys count %d%n", keysCount);
    NavigableMap<EdgeKey, Integer> keyValues = new TreeMap<>();

    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMinor: " + seed);
    final Random random = new Random(seed);

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final int val = random.nextInt(Integer.MAX_VALUE);

            EdgeKey key = new EdgeKey(42, val, val % 64937);
            bTree.put(atomicOperation, key, val);
            keyValues.put(key, val);
          });

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(bTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(bTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMinorEntries(
      NavigableMap<EdgeKey, Integer> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    EdgeKey[] keys = new EdgeKey[keyValues.size()];
    int index = 0;

    for (EdgeKey key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int toKeyIndex = random.nextInt(keys.length);
      EdgeKey toKey = keys[toKeyIndex];
      if (random.nextBoolean()) {
        toKey = new EdgeKey(toKey.ridBagId, toKey.targetCluster, toKey.targetPosition + 1);
      }

      final Iterator<ORawPair<EdgeKey, Integer>> indexIterator;
      try (Stream<ORawPair<EdgeKey, Integer>> stream =
          bTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<EdgeKey, Integer>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
        } else {
          iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
        }

        while (iterator.hasNext()) {
          ORawPair<EdgeKey, Integer> indexEntry = indexIterator.next();
          Map.Entry<EdgeKey, Integer> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  @Test
  public void testIterateEntriesBetween() throws Exception {
    System.out.printf("Keys count %d%n", keysCount);

    NavigableMap<EdgeKey, Integer> keyValues = new TreeMap<>();
    final Random random = new Random();

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            int val = random.nextInt(Integer.MAX_VALUE);
            EdgeKey key = new EdgeKey(42, val, val % 64937);
            bTree.put(atomicOperation, key, val);
            keyValues.put(key, val);
          });

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }
    assertIterateBetweenEntries(keyValues, random, true, true, true);
    assertIterateBetweenEntries(keyValues, random, true, false, true);
    assertIterateBetweenEntries(keyValues, random, false, true, true);
    assertIterateBetweenEntries(keyValues, random, false, false, true);

    assertIterateBetweenEntries(keyValues, random, true, true, false);
    assertIterateBetweenEntries(keyValues, random, true, false, false);
    assertIterateBetweenEntries(keyValues, random, false, true, false);
    assertIterateBetweenEntries(keyValues, random, false, false, false);

    Assert.assertEquals(bTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(bTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateBetweenEntries(
      NavigableMap<EdgeKey, Integer> keyValues,
      Random random,
      boolean fromInclusive,
      boolean toInclusive,
      boolean ascSortOrder) {
    EdgeKey[] keys = new EdgeKey[keyValues.size()];
    int index = 0;

    for (EdgeKey key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int fromKeyIndex = random.nextInt(keys.length);
      int toKeyIndex = random.nextInt(keys.length);

      if (fromKeyIndex > toKeyIndex) {
        toKeyIndex = fromKeyIndex;
      }

      EdgeKey fromKey = keys[fromKeyIndex];
      EdgeKey toKey = keys[toKeyIndex];

      if (random.nextBoolean()) {
        fromKey = new EdgeKey(fromKey.ridBagId, fromKey.targetCluster, fromKey.targetPosition - 1);
      }

      if (random.nextBoolean()) {
        toKey = new EdgeKey(toKey.ridBagId, toKey.targetCluster, toKey.targetPosition + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      final Iterator<ORawPair<EdgeKey, Integer>> indexIterator;
      try (Stream<ORawPair<EdgeKey, Integer>> stream =
          bTree.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<EdgeKey, Integer>> iterator;
        if (ascSortOrder) {
          iterator =
              keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
        } else {
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(toKey, toInclusive, fromKey, fromInclusive)
                  .entrySet()
                  .iterator();
        }

        while (iterator.hasNext()) {
          ORawPair<EdgeKey, Integer> indexEntry = indexIterator.next();
          Assert.assertNotNull(indexEntry);

          Map.Entry<EdgeKey, Integer> mapEntry = iterator.next();
          Assert.assertEquals(indexEntry.first, mapEntry.getKey());
          Assert.assertEquals(indexEntry.second, mapEntry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }
}
