package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.stream.Stream;

public class CellBTreeSingleValueV1TestIT {
  private OAbstractPaginatedStorage      storage;
  private CellBTreeSingleValueV1<String> singleValueTree;
  private OrientDB                       orientDB;

  private String dbName;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", ".") + File.separator + CellBTreeSingleValueV1TestIT.class.getSimpleName();

    dbName = "localSingleBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    OrientDBConfig config = OrientDBConfig.builder().addConfig(OGlobalConfiguration.STORAGE_TRACK_PAGE_OPERATIONS_IN_TX, true)
        .build();
    orientDB = new OrientDB("plocal:" + buildDirectory, config);
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    try (ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin")) {
      storage = (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage();
    }
    singleValueTree = new CellBTreeSingleValueV1<>("singleBTree", ".sbt", ".nbt", storage);
    singleValueTree.create(OUTF8Serializer.INSTANCE, null, 1, null);
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() throws Exception {
    final int keysCount = 1_000_000;

    final int rollbackInterval = 100;
    String lastKey = null;
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          final String key = Integer.toString(i * rollbackInterval + j);
          singleValueTree.put(key, new ORecordId((i * rollbackInterval + j) % 32000, i * rollbackInterval + j));

          if (n == 1) {
            if ((i * rollbackInterval + j) % 100_000 == 0) {
              System.out.printf("%d items loaded out of %d%n", i * rollbackInterval + j, keysCount);
            }

            if (lastKey == null) {
              lastKey = key;
            } else if (key.compareTo(lastKey) > 0) {
              lastKey = key;
            }
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }

      Assert.assertEquals("0", singleValueTree.firstKey());
      Assert.assertEquals(lastKey, singleValueTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(i + " key is absent", new ORecordId(i % 32000, i), singleValueTree.get(Integer.toString(i)));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d%n", i, keysCount);
      }
    }

    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertNull(singleValueTree.get(Integer.toString(i)));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<String> keys = new TreeSet<>();
    final Random random = new Random();
    final int keysCount = 1_000_000;

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    final int rollbackRange = 100;
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int i = 0; i < rollbackRange; i++) {
          int val = random.nextInt(Integer.MAX_VALUE);
          String key = Integer.toString(val);

          singleValueTree.put(key, new ORecordId(val % 32000, val));

          if (n == 1) {
            keys.add(key);
          }
          Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      final int val = Integer.parseInt(key);
      Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 1_000_000;
    final int rollbackRange = 100;

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int i = 0; i < rollbackRange; i++) {
          int val;
          do {
            val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
          } while (val < 0);

          String key = Integer.toString(val);
          singleValueTree.put(key, new ORecordId(val % 32000, val));
          if (n == 1) {
            keys.add(key);
          }

          Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    final int keysCount = 1_000_000;

    NavigableSet<String> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      String key = Integer.toString(i);
      singleValueTree.put(key, new ORecordId(i % 32000, i));
      keys.add(key);
    }

    final int rollbackInterval = 10;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    Iterator<String> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        singleValueTree.remove(key);
        keysIterator.remove();
      }

      atomicOperationsManager.startAtomicOperation((String) null, false);
      int rollbackCounter = 0;
      final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
      while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
        String keyToDelete = keysDeletionIterator.next();
        rollbackCounter++;
        singleValueTree.remove(keyToDelete);
      }
      atomicOperationsManager.endAtomicOperation(true);
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(singleValueTree.get(key));
      } else {
        Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();
    final int keysCount = 1_000_000;

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }

      String key = Integer.toString(val);
      singleValueTree.put(key, new ORecordId(val % 32000, val));
      keys.add(key);

      Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
    }

    Iterator<String> keysIterator = keys.iterator();

    final int rollbackInterval = 10;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        singleValueTree.remove(key);
        keysIterator.remove();
      }

      atomicOperationsManager.startAtomicOperation((String) null, false);
      int rollbackCounter = 0;
      final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
      while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
        String keyToDelete = keysDeletionIterator.next();
        rollbackCounter++;
        singleValueTree.remove(keyToDelete);
      }
      atomicOperationsManager.endAtomicOperation(true);
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(singleValueTree.get(key));
      } else {
        Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      singleValueTree.put(Integer.toString(i), new ORecordId(i % 32000, i));
    }

    final int rollbackInterval = 100;

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          final int key = i * rollbackInterval + j;
          if (key % 3 == 0) {
            Assert.assertEquals(singleValueTree.remove(Integer.toString(key)), new ORecordId(key % 32000, key));
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      singleValueTree.put(Integer.toString(i), new ORecordId(i % 32000, i));

      Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
    }

    final int rollbackInterval = 100;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);

        for (int j = 0; j < rollbackInterval; j++) {
          final int key = i * rollbackInterval + j;

          if (key % 3 == 0) {
            Assert.assertEquals(singleValueTree.remove(Integer.toString(key)), new ORecordId(key % 32000, key));
          }

          if (key % 2 == 0) {
            singleValueTree.put(Integer.toString(keysCount + key), new ORecordId((keysCount + key) % 32000, keysCount + key));
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(singleValueTree.get(Integer.toString(keysCount + i)),
            new ORecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testKeyCursor() throws Exception {
    final int keysCount = 1_000_000;

    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testKeyCursor: " + seed);
    Random random = new Random(seed);

    final int rollbackInterval = 100;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    int printCounter = 0;
    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          int val = random.nextInt(Integer.MAX_VALUE);
          String key = Integer.toString(val);

          singleValueTree.put(key, new ORecordId(val % 32000, val));
          if (n == 1) {
            keyValues.put(key, new ORecordId(val % 32000, val));
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());

    final Iterator<String> indexIterator;
    try (Stream<String> stream = singleValueTree.keyStream()) {
      indexIterator = stream.iterator();
      for (String entryKey : keyValues.keySet()) {
        final String indexKey = indexIterator.next();
        Assert.assertEquals(entryKey, indexKey);
      }
    }
  }

  @Test
  public void testIterateEntriesMajor() throws Exception {
    final int keysCount = 1_000_000;

    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMajor: " + seed);
    final Random random = new Random(seed);

    final int rollbackInterval = 100;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          int val = random.nextInt(Integer.MAX_VALUE);
          String key = Integer.toString(val);

          singleValueTree.put(key, new ORecordId(val % 32000, val));
          if (n == 1) {
            keyValues.put(key, new ORecordId(val % 32000, val));
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesMinor() throws Exception {
    final int keysCount = 1_000_000;
    NavigableMap<String, ORID> keyValues = new TreeMap<>();

    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMinor: " + seed);
    final Random random = new Random(seed);

    final int rollbackInterval = 100;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          int val = random.nextInt(Integer.MAX_VALUE);
          String key = Integer.toString(val);

          singleValueTree.put(key, new ORecordId(val % 32000, val));
          if (n == 1) {
            keyValues.put(key, new ORecordId(val % 32000, val));
          }
        }
        atomicOperationsManager.endAtomicOperation(n == 0);
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesBetween() throws Exception {
    final int keysCount = 1_000_000;
    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final Random random = new Random();

    final int rollbackInterval = 100;
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);
        for (int j = 0; j < rollbackInterval; j++) {
          int val = random.nextInt(Integer.MAX_VALUE);
          String key = Integer.toString(val);

          singleValueTree.put(key, new ORecordId(val % 32000, val));
          if (n == 1) {
            keyValues.put(key, new ORecordId(val % 32000, val));
          }
        }

        atomicOperationsManager.endAtomicOperation(n == 0);
      }

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

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(NavigableMap<String, ORID> keyValues, Random random, boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      final int fromKeyIndex = random.nextInt(keys.length);
      String fromKey = keys[fromKeyIndex];

      if (random.nextBoolean()) {
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (Stream<ORawPair<String, ORID>> stream = singleValueTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();
        Iterator<Map.Entry<String, ORID>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
        } else {
          iterator = keyValues.descendingMap().subMap(keyValues.lastKey(), true, fromKey, keyInclusive).entrySet().iterator();
        }

        while (iterator.hasNext()) {
          final ORawPair<String, ORID> indexEntry = indexIterator.next();
          final Map.Entry<String, ORID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }

    }
  }

  private void assertIterateMinorEntries(NavigableMap<String, ORID> keyValues, Random random, boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int toKeyIndex = random.nextInt(keys.length);
      String toKey = keys[toKeyIndex];
      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (Stream<ORawPair<String, ORID>> stream = singleValueTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();
        Iterator<Map.Entry<String, ORID>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
        } else {
          iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
        }

        while (iterator.hasNext()) {
          ORawPair<String, ORID> indexEntry = indexIterator.next();
          Map.Entry<String, ORID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }

    }
  }

  private void assertIterateBetweenEntries(NavigableMap<String, ORID> keyValues, Random random, boolean fromInclusive,
      boolean toInclusive, boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int fromKeyIndex = random.nextInt(keys.length);
      int toKeyIndex = random.nextInt(keys.length);

      if (fromKeyIndex > toKeyIndex) {
        toKeyIndex = fromKeyIndex;
      }

      String fromKey = keys[fromKeyIndex];
      String toKey = keys[toKeyIndex];

      if (random.nextBoolean()) {
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (Stream<ORawPair<String, ORID>> stream = singleValueTree
          .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<String, ORID>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
        } else {
          iterator = keyValues.descendingMap().subMap(toKey, toInclusive, fromKey, fromInclusive).entrySet().iterator();
        }

        while (iterator.hasNext()) {
          ORawPair<String, ORID> indexEntry = indexIterator.next();
          Assert.assertNotNull(indexEntry);

          Map.Entry<String, ORID> mapEntry = iterator.next();
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
