package com.orientechnologies.orient.core.storage.index.sbtree.local.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.08.13
 */
public class SBTreeV1TestIT {
  private int keysCount = 1_000_000;
  OSBTreeV1<Integer, OIdentifiable> sbTree;
  protected ODatabaseSession databaseDocumentTx;
  protected String buildDirectory;
  protected OrientDB orientDB;
  protected OAbstractPaginatedStorage storage;
  protected OAtomicOperationsManager atomicOperationsManager;

  String dbName;

  @Before
  public void before() throws Exception {
    buildDirectory =
        System.getProperty("buildDirectory", ".")
            + File.separator
            + SBTreeV1TestIT.class.getSimpleName();

    try {
      keysCount =
          Integer.parseInt(
              System.getProperty(
                  SBTreeV1TestIT.class.getSimpleName() + "KeysCount", Integer.toString(keysCount)));
    } catch (NumberFormatException e) {
      // ignore
    }

    System.out.println("keysCount parameter is set to " + keysCount);

    dbName = "localSBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    OrientDBConfig orientDBConfig =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_TRACK_PAGE_OPERATIONS_IN_TX, true)
            .build();
    orientDB = new OrientDB("plocal:" + buildDirectory, orientDBConfig);
    orientDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();

    sbTree = new OSBTreeV1<>("sbTree", ".sbt", ".nbt", storage);
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(
                atomicOperation,
                OIntegerSerializer.INSTANCE,
                OLinkSerializer.INSTANCE,
                null,
                1,
                false,
                null));
  }

  @After
  public void afterMethod() throws Exception {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() throws Exception {
    final int rollbackInterval = 100;
    Integer[] lastKey = new Integer[1];
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int primaryCounter = i;
        final int subCounter = n;

        final OAtomicOperationsManager atomicOperationsManager =
            storage.getAtomicOperationsManager();
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final Integer key = primaryCounter * rollbackInterval + j;
                  sbTree.put(
                      atomicOperation,
                      key,
                      new ORecordId(
                          (primaryCounter * rollbackInterval + j) % 32000,
                          primaryCounter * rollbackInterval + j));

                  if (primaryCounter == 1) {
                    if ((subCounter * rollbackInterval + j) % 100_000 == 0) {
                      System.out.printf(
                          "%d items loaded out of %d%n",
                          primaryCounter * rollbackInterval + j, keysCount);
                    }

                    if (lastKey[0] == null) {
                      lastKey[0] = key;
                    } else if (key.compareTo(lastKey[0]) > 0) {
                      lastKey[0] = key;
                    }
                  }
                }
                if (subCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      final Integer firstTreeKey = sbTree.firstKey();
      final Integer lastTreeKey = sbTree.lastKey();

      Assert.assertNotNull(firstTreeKey);
      Assert.assertNotNull(lastTreeKey);

      Assert.assertEquals(0, (int) firstTreeKey);
      Assert.assertEquals(lastTreeKey, lastTreeKey);
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(i + " key is absent", new ORecordId(i % 32000, i), sbTree.get(i));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d%n", i, keysCount);
      }
    }

    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertNull(sbTree.get(i));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<>();
    final Random random = new Random();
    final int keysCount = 1_000_000;

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    final int rollbackRange = 100;
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int primaryCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int key = random.nextInt(Integer.MAX_VALUE);
                  sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key));

                  if (primaryCounter == 1) {
                    keys.add(key);
                  }
                  Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
                }
                if (primaryCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    Integer firstTreeKey = sbTree.firstKey();
    Integer lastTreeKey = sbTree.lastKey();
    Assert.assertNotNull(firstTreeKey);
    Assert.assertNotNull(lastTreeKey);

    Assert.assertEquals(firstTreeKey, keys.first());
    Assert.assertEquals(lastTreeKey, keys.last());

    for (Integer key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 1_000_000;
    final int rollbackRange = 100;

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int counter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val;
                  do {
                    val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                  } while (val < 0);

                  sbTree.put(atomicOperation, val, new ORecordId(val % 32000, val));
                  if (counter == 1) {
                    keys.add(val);
                  }

                  Assert.assertEquals(sbTree.get(val), new ORecordId(val % 32000, val));
                }
                if (counter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    Integer firstKey = sbTree.firstKey();
    Integer lastKey = sbTree.lastKey();

    Assert.assertNotNull(firstKey);
    Assert.assertNotNull(lastKey);

    Assert.assertEquals(firstKey, keys.first());
    Assert.assertEquals(lastKey, keys.last());

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    final int keysCount = 1_000_000;

    NavigableSet<Integer> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      final int counter = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            sbTree.put(atomicOperation, counter, new ORecordId(counter % 32000, counter));
            keys.add(counter);
          });
    }

    final int rollbackInterval = 10;

    Iterator<Integer> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      Integer key = keysIterator.next();

      if (key % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sbTree.remove(atomicOperation, key));
        keysIterator.remove();
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<Integer> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                Integer keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                sbTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
    }

    final Integer firstKey = sbTree.firstKey();
    final Integer lastKey = sbTree.lastKey();

    Assert.assertNotNull(firstKey);
    Assert.assertNotNull(lastKey);

    Assert.assertEquals(firstKey, keys.first());
    Assert.assertEquals(lastKey, keys.last());

    for (Integer key : keys) {
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
    final int keysCount = 1_000_000;

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, val, new ORecordId(val % 32000, val)));
      keys.add(val);

      Assert.assertEquals(sbTree.get(val), new ORecordId(val % 32000, val));
    }

    Iterator<Integer> keysIterator = keys.iterator();

    final int rollbackInterval = 10;
    while (keysIterator.hasNext()) {
      Integer key = keysIterator.next();

      if (key % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sbTree.remove(atomicOperation, key));
        keysIterator.remove();
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<Integer> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                Integer keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                sbTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
    }

    final Integer firstKey = sbTree.firstKey();
    final Integer lastKey = sbTree.lastKey();

    Assert.assertNotNull(firstKey);
    Assert.assertNotNull(lastKey);

    Assert.assertEquals(firstKey, keys.first());
    Assert.assertEquals(lastKey, keys.last());

    for (Integer key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
      } else {
        Assert.assertEquals(sbTree.get(key), new ORecordId(key % 32000, key));
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    for (int i = 0; i < keysCount; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key)));
    }

    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int primaryCounter = i;
        final int subCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = primaryCounter * rollbackInterval + j;
                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        sbTree.remove(atomicOperation, key), new ORecordId(key % 32000, key));
                  }
                }
                if (subCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(sbTree.get(i));
      } else {
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < keysCount; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key)));

      Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
    }

    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = iterationCounter * rollbackInterval + j;

                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        sbTree.remove(atomicOperation, key), new ORecordId(key % 32000, key));
                  }

                  if (key % 2 == 0) {
                    sbTree.put(
                        atomicOperation,
                        keysCount + key,
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
        Assert.assertNull(sbTree.get(i));
      } else {
        Assert.assertEquals(sbTree.get(i), new ORecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(
            sbTree.get(keysCount + i), new ORecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testIterateEntriesMajor() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();
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
                  sbTree.put(atomicOperation, val, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(val, new ORecordId(val % 32000, val));
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

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesMinor() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();

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
                  sbTree.put(atomicOperation, val, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(val, new ORecordId(val % 32000, val));
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

    final Integer firstTreeKey = sbTree.firstKey();
    final Integer lastTreeKey = sbTree.lastKey();

    Assert.assertNotNull(firstTreeKey);
    Assert.assertNotNull(lastTreeKey);

    Assert.assertEquals(firstTreeKey, keyValues.firstKey());
    Assert.assertEquals(lastTreeKey, keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesBetween() throws Exception {
    NavigableMap<Integer, ORID> keyValues = new TreeMap<>();
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
                  sbTree.put(atomicOperation, val, new ORecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(val, new ORecordId(val % 32000, val));
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

    final Integer firstKey = sbTree.firstKey();
    final Integer lastKey = sbTree.lastKey();

    Assert.assertNotNull(firstKey);
    Assert.assertNotNull(lastKey);

    Assert.assertEquals(firstKey, keyValues.firstKey());
    Assert.assertEquals(lastKey, keyValues.lastKey());
  }

  @Test
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    for (int i = 0; i < 5167; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key)));
    }

    for (int i = 0; i < 3500; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    final Integer firstKey = sbTree.firstKey();
    Assert.assertNotNull(firstKey);
    Assert.assertEquals((int) firstKey, 3500);
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
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key)));
    }

    for (int i = 5166; i > 1700; i--) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    final Integer lastKey = sbTree.lastKey();
    Assert.assertNotNull(lastKey);
    Assert.assertEquals((int) lastKey, 1700);

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
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new ORecordId(key % 32000, key)));
    }

    for (int i = 0; i < 1730; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    for (int i = 3440; i < 6900; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    for (int i = 8600; i < 12055; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    final Integer firstKey = sbTree.firstKey();
    Assert.assertNotNull(firstKey);
    Assert.assertEquals((int) firstKey, 1730);

    final Integer lastKey = sbTree.lastKey();
    Assert.assertNotNull(lastKey);
    Assert.assertEquals((int) lastKey, 8599);

    Set<OIdentifiable> identifiables = new HashSet<>();

    Stream<ORawPair<Integer, OIdentifiable>> stream = sbTree.iterateEntriesMinor(7200, true, true);
    streamToSet(identifiables, stream);

    for (int i = 7200; i >= 6900; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 3439; i >= 1730; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    stream = sbTree.iterateEntriesMinor(7200, true, false);
    streamToSet(identifiables, stream);

    for (int i = 7200; i >= 6900; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 3439; i >= 1730; i--) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    stream = sbTree.iterateEntriesMajor(1740, true, true);
    streamToSet(identifiables, stream);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i < 8600; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    stream = sbTree.iterateEntriesMajor(1740, true, false);
    streamToSet(identifiables, stream);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i < 8600; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    stream = sbTree.iterateEntriesBetween(1740, true, 7200, true, true);
    streamToSet(identifiables, stream);

    for (int i = 1740; i < 3440; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 6900; i <= 7200; i++) {
      boolean removed = identifiables.remove(new ORecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    stream = sbTree.iterateEntriesBetween(1740, true, 7200, true, false);
    streamToSet(identifiables, stream);

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
    final OSBTreeV1<Integer, OIdentifiable> nullSBTree =
        new OSBTreeV1<>(
            "nullSBTree",
            ".sbt",
            ".nbt",
            (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage());
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            nullSBTree.create(
                atomicOperation,
                OIntegerSerializer.INSTANCE,
                OLinkSerializer.INSTANCE,
                null,
                1,
                true,
                null));

    try {
      for (int i = 0; i < 10; i++) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> nullSBTree.put(atomicOperation, key, new ORecordId(3, key)));
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              doNullTesting(atomicOperation, nullSBTree);
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> doNullTesting(atomicOperation, nullSBTree));

    } finally {
      try (Stream<Integer> keyStream = nullSBTree.keyStream()) {

        keyStream.forEach(
            (key) -> {
              try {
                atomicOperationsManager.executeInsideAtomicOperation(
                    null, atomicOperation -> nullSBTree.remove(atomicOperation, key));
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            });
      }

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            nullSBTree.remove(atomicOperation, null);
            nullSBTree.delete(atomicOperation);
          });
    }
  }

  private static void doNullTesting(
      OAtomicOperation atomicOperation, OSBTreeV1<Integer, OIdentifiable> nullSBTree) {
    OIdentifiable identifiable = nullSBTree.get(null);
    Assert.assertNull(identifiable);

    nullSBTree.put(atomicOperation, null, new ORecordId(10, 1000));

    identifiable = nullSBTree.get(null);
    Assert.assertEquals(identifiable, new ORecordId(10, 1000));

    OIdentifiable removed = nullSBTree.remove(atomicOperation, 5);
    Assert.assertEquals(removed, new ORecordId(3, 5));

    removed = nullSBTree.remove(atomicOperation, null);
    Assert.assertEquals(removed, new ORecordId(10, 1000));

    removed = nullSBTree.remove(atomicOperation, null);
    Assert.assertNull(removed);

    identifiable = nullSBTree.get(null);
    Assert.assertNull(identifiable);
  }

  private static void streamToSet(
      Set<OIdentifiable> identifiables, Stream<ORawPair<Integer, OIdentifiable>> stream) {
    identifiables.clear();
    identifiables.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }

  private void assertIterateMajorEntries(
      NavigableMap<Integer, ORID> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0) fromKey = random.nextInt(upperBorder);
      else fromKey = random.nextInt(Integer.MAX_VALUE);

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null) fromKey = includedKey;
        else fromKey = keyValues.floorKey(fromKey);
      }

      Iterator<Map.Entry<Integer, ORID>> iterator;
      final Iterator<ORawPair<Integer, OIdentifiable>> indexIterator;
      try (Stream<ORawPair<Integer, OIdentifiable>> stream =
          sbTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder)) {

        if (ascSortOrder) iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
        else
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(keyValues.lastKey(), true, fromKey, keyInclusive)
                  .entrySet()
                  .iterator();

        indexIterator = stream.iterator();

        while (iterator.hasNext()) {
          final ORawPair<Integer, OIdentifiable> indexEntry = indexIterator.next();
          final Map.Entry<Integer, ORID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  private void assertIterateMinorEntries(
      NavigableMap<Integer, ORID> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int toKey;
      if (upperBorder > 0) toKey = random.nextInt(upperBorder) - 5000;
      else toKey = random.nextInt(Integer.MAX_VALUE) - 5000;

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null) toKey = includedKey;
        else toKey = keyValues.floorKey(toKey);
      }

      final Iterator<ORawPair<Integer, OIdentifiable>> indexIterator;
      try (Stream<ORawPair<Integer, OIdentifiable>> stream =
          sbTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<Integer, ORID>> iterator;
        if (ascSortOrder) iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
        else
          iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();

        while (iterator.hasNext()) {
          ORawPair<Integer, OIdentifiable> indexEntry = indexIterator.next();
          Map.Entry<Integer, ORID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  private void assertIterateBetweenEntries(
      NavigableMap<Integer, ORID> keyValues,
      Random random,
      boolean fromInclusive,
      boolean toInclusive,
      boolean ascSortOrder) {
    long totalTime = 0;
    long totalIterations = 0;

    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0) fromKey = random.nextInt(upperBorder);
      else fromKey = random.nextInt(Integer.MAX_VALUE - 1);

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null) fromKey = includedKey;
        else fromKey = keyValues.floorKey(fromKey);
      }

      int toKey = random.nextInt() + fromKey + 1;
      if (toKey < 0) toKey = Integer.MAX_VALUE;

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null) toKey = includedKey;
        else toKey = keyValues.floorKey(toKey);
      }

      if (fromKey > toKey) toKey = fromKey;

      Iterator<ORawPair<Integer, OIdentifiable>> indexIterator;
      try (Stream<ORawPair<Integer, OIdentifiable>> stream =
          sbTree.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<Integer, ORID>> iterator;
        if (ascSortOrder)
          iterator =
              keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
        else
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(toKey, toInclusive, fromKey, fromInclusive)
                  .entrySet()
                  .iterator();

        long startTime = System.currentTimeMillis();
        int iteration = 0;
        while (iterator.hasNext()) {
          iteration++;

          ORawPair<Integer, OIdentifiable> indexEntry = indexIterator.next();
          Assert.assertNotNull(indexEntry);

          Map.Entry<Integer, ORID> mapEntry = iterator.next();
          Assert.assertEquals(indexEntry.first, mapEntry.getKey());
          Assert.assertEquals(indexEntry.second, mapEntry.getValue());
        }

        long endTime = System.currentTimeMillis();

        totalIterations += iteration;
        totalTime += (endTime - startTime);

        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }

    if (totalTime != 0)
      System.out.println("Iterations per second : " + (totalIterations * 1000) / totalTime);
  }

  static final class RollbackException extends OException implements OHighLevelException {
    public RollbackException() {
      this("");
    }

    public RollbackException(String message) {
      super(message);
    }

    @SuppressWarnings("unused")
    public RollbackException(RollbackException exception) {
      super(exception);
    }
  }
}
