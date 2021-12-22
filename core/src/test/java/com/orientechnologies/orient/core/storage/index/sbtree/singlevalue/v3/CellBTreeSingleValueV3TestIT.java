package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
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
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CellBTreeSingleValueV3TestIT {
  private OAtomicOperationsManager atomicOperationsManager;
  private CellBTreeSingleValueV3<String> singleValueTree;
  private OrientDB orientDB;

  private String dbName;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", ".")
            + File.separator
            + CellBTreeSingleValueV3TestIT.class.getSimpleName();

    dbName = "localSingleBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    final OrientDBConfig config = OrientDBConfig.builder().build();
    orientDB = new OrientDB("plocal:" + buildDirectory, config);
    orientDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

    OAbstractPaginatedStorage storage;
    try (ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin")) {
      storage =
          (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
    }
    singleValueTree = new CellBTreeSingleValueV3<>("singleBTree", ".sbt", ".nbt", storage);
    atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            singleValueTree.create(atomicOperation, OUTF8Serializer.INSTANCE, null, 1, null));
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
    String[] lastKey = new String[1];
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final String key = Integer.toString(iterationCounter * rollbackInterval + j);
                  singleValueTree.put(
                      atomicOperation,
                      key,
                      new ORecordId(
                          (iterationCounter * rollbackInterval + j) % 32000,
                          iterationCounter * rollbackInterval + j));

                  if (rollbackCounter == 1) {
                    if ((iterationCounter * rollbackInterval + j) % 100_000 == 0) {
                      System.out.printf(
                          "%d items loaded out of %d%n",
                          iterationCounter * rollbackInterval + j, keysCount);
                    }

                    if (lastKey[0] == null) {
                      lastKey[0] = key;
                    } else if (key.compareTo(lastKey[0]) > 0) {
                      lastKey[0] = key;
                    }
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
      Assert.assertEquals("0", singleValueTree.firstKey());
      Assert.assertEquals(lastKey[0], singleValueTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(
          i + " key is absent",
          new ORecordId(i % 32000, i),
          singleValueTree.get(Integer.toString(i)));
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

    final int rollbackRange = 100;
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);
                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));

                  if (rollbackCounter == 1) {
                    keys.add(key);
                  }
                  Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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

    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val;
                  do {
                    val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                  } while (val < 0);

                  String key = Integer.toString(val);
                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keys.add(key);
                  }
                  Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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
      final int k = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(atomicOperation, key, new ORecordId(k % 32000, k)));
      keys.add(key);
    }

    final int rollbackInterval = 10;
    Iterator<String> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();
      if (Integer.parseInt(key) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> singleValueTree.remove(atomicOperation, key));
        keysIterator.remove();
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                String keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                singleValueTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
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
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val)));
      keys.add(key);

      Assert.assertEquals(singleValueTree.get(key), new ORecordId(val % 32000, val));
    }
    Iterator<String> keysIterator = keys.iterator();

    final int rollbackInterval = 10;
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> singleValueTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                String keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                singleValueTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
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
      final int k = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(
                  atomicOperation, Integer.toString(k), new ORecordId(k % 32000, k)));
    }
    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        final int iterationsCounter = i;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = iterationsCounter * rollbackInterval + j;
                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        singleValueTree.remove(atomicOperation, Integer.toString(key)),
                        new ORecordId(key % 32000, key));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(
                  atomicOperation, Integer.toString(key), new ORecordId(key % 32000, key)));

      Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
    }
    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        final int iterationsCounter = i;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = iterationsCounter * rollbackInterval + j;

                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        singleValueTree.remove(atomicOperation, Integer.toString(key)),
                        new ORecordId(key % 32000, key));
                  }

                  if (key % 2 == 0) {
                    singleValueTree.put(
                        atomicOperation,
                        Integer.toString(keysCount + key),
                        new ORecordId((keysCount + key) % 32000, keysCount + key));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(
            singleValueTree.get(Integer.toString(keysCount + i)),
            new ORecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testKeyAddDeleteAll() throws Exception {
    for (int iterations = 0; iterations < 4; iterations++) {
      System.out.println("testKeyAddDeleteAll : iteration " + iterations);

      final int keysCount = 1_000_000;

      for (int i = 0; i < keysCount; i++) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                singleValueTree.put(
                    atomicOperation, Integer.toString(key), new ORecordId(key % 32000, key)));

        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
      }

      for (int i = 0; i < keysCount; i++) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              Assert.assertEquals(
                  singleValueTree.remove(atomicOperation, Integer.toString(key)),
                  new ORecordId(key % 32000, key));

              if (key > 0 && key % 100_000 == 0) {
                for (int keyToVerify = 0; keyToVerify < keysCount; keyToVerify++) {
                  if (keyToVerify > key) {
                    Assert.assertEquals(
                        new ORecordId(keyToVerify % 32000, keyToVerify),
                        singleValueTree.get(Integer.toString(keyToVerify)));
                  } else {
                    Assert.assertNull(singleValueTree.get(Integer.toString(keyToVerify)));
                  }
                }
              }
            });
      }
      for (int i = 0; i < keysCount; i++) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      }

      singleValueTree.assertFreePages(atomicOperationsManager.getCurrentOperation());
    }
  }

  @Test
  public void testKeyAddDeleteHalf() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount / 2; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(
                  atomicOperation, Integer.toString(key), new ORecordId(key % 32000, key)));

      Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new ORecordId(i % 32000, i));
    }

    for (int iterations = 0; iterations < 4; iterations++) {
      System.out.println("testKeyAddDeleteHalf : iteration " + iterations);

      for (int i = 0; i < keysCount / 2; i++) {
        final int key = i + (iterations + 1) * keysCount / 2;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                singleValueTree.put(
                    atomicOperation, Integer.toString(key), new ORecordId(key % 32000, key)));

        Assert.assertEquals(
            singleValueTree.get(Integer.toString(key)), new ORecordId(key % 32000, key));
      }

      final int offset = iterations * (keysCount / 2);

      for (int i = 0; i < keysCount / 2; i++) {
        final int key = i + offset;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    singleValueTree.remove(atomicOperation, Integer.toString(key)),
                    new ORecordId(key % 32000, key)));
      }

      final int start = (iterations + 1) * (keysCount / 2);
      for (int i = 0; i < (iterations + 2) * keysCount / 2; i++) {
        if (i < start) {
          Assert.assertNull(singleValueTree.get(Integer.toString(i)));
        } else {
          Assert.assertEquals(
              new ORecordId(i % 32000, i), singleValueTree.get(Integer.toString(i)));
        }
      }

      singleValueTree.assertFreePages(atomicOperationsManager.getCurrentOperation());
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

    int printCounter = 0;
    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new ORecordId(val % 32000, val));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new ORecordId(val % 32000, val));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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
    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new ORecordId(val % 32000, val));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new ORecordId(val % 32000, val));
                  }
                }

                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
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

  @Test
  public void testIterateEntriesBetweenString() throws Exception {
    final int keysCount = 10;
    final NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final Random random = new Random();
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int j = 0; j < keysCount; j++) {
              final String key = "name" + j;
              final int val = random.nextInt(Integer.MAX_VALUE);
              final int clusterId = val % 32000;
              singleValueTree.put(atomicOperation, key, new ORecordId(clusterId, val));
              System.out.println("Added key=" + key + ", value=" + val);

              keyValues.put(key, new ORecordId(clusterId, val));
            }
          });
    } catch (final RollbackException ignore) {
      Assert.fail();
    }
    assertIterateBetweenEntriesNonRandom("name5", keyValues, true, true, true, 5);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(
      NavigableMap<String, ORID> keyValues,
      Random random,
      boolean keyInclusive,
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
        fromKey =
            fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (Stream<ORawPair<String, ORID>> stream =
          singleValueTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<String, ORID>> iterator;
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

  private void assertIterateMinorEntries(
      NavigableMap<String, ORID> keyValues,
      Random random,
      boolean keyInclusive,
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
      try (Stream<ORawPair<String, ORID>> stream =
          singleValueTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder)) {
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

  private void assertIterateBetweenEntries(
      NavigableMap<String, ORID> keyValues,
      Random random,
      boolean fromInclusive,
      boolean toInclusive,
      boolean ascSortOrder) {
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
        fromKey =
            fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (Stream<ORawPair<String, ORID>> stream =
          singleValueTree.iterateEntriesBetween(
              fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<String, ORID>> iterator;
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

  private void assertIterateBetweenEntriesNonRandom(
      final String fromKey,
      final NavigableMap<String, ORID> keyValues,
      final boolean fromInclusive,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final int startFrom) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (final String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = startFrom; i < keyValues.size(); i++) {
      final String toKey = keys[i];
      final Iterator<ORawPair<String, ORID>> indexIterator;
      try (final Stream<ORawPair<String, ORID>> stream =
          singleValueTree.iterateEntriesBetween(
              fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();
        Assert.assertTrue(indexIterator.hasNext());
      }
    }
  }

  static final class RollbackException extends OException implements OHighLevelException {
    @SuppressWarnings("WeakerAccess")
    public RollbackException() {
      this("");
    }

    @SuppressWarnings("WeakerAccess")
    public RollbackException(String message) {
      super(message);
    }

    @SuppressWarnings("unused")
    public RollbackException(RollbackException exception) {
      super(exception);
    }
  }
}
