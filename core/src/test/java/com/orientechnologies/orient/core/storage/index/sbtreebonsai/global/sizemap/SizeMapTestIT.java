package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SizeMapTestIT {

  public static final String DIR_NAME = "/sizeMapTest";
  public static final String DB_NAME = "sizeMapTest";
  private static OrientDB orientDB;
  private static OAtomicOperationsManager atomicOperationsManager;
  private static OAbstractPaginatedStorage storage;
  private static String buildDirectory;

  private SizeMap sizeMap;

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

    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

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
  public void setUp() throws Exception {
    sizeMap = new SizeMap(storage, "sizeMap", ".sm");
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> sizeMap.create(atomicOperation));
  }

  @After
  public void tearDown() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> sizeMap.delete(atomicOperation));
  }

  @Test
  public void createSingleItem() throws Exception {
    final int id =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null, atomicOperation -> sizeMap.addTree(atomicOperation));

    Assert.assertEquals(0, id);
  }

  @Test
  public void incrementSize() throws Exception {
    final long seed = System.nanoTime();
    System.out.printf("incrementSize seed :%d%n", seed);
    final Random rnd = new Random(seed);

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.addTree(atomicOperation));
    }

    final int[] counters = new int[2 * Bucket.MAX_BUCKET_SIZE];

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      final int index = i;

      final int increments = rnd.nextInt(11);

      for (int j = 0; j < increments; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.incrementSize(atomicOperation, index));
      }

      counters[i] = increments;
    }

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      Assert.assertEquals(counters[i], sizeMap.getSize(i));
    }
  }

  @Test
  public void decrementSize() throws Exception {
    final long seed = System.nanoTime();
    System.out.printf("decrementSize seed :%d%n", seed);
    final Random rnd = new Random(seed);

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.addTree(atomicOperation));
    }

    final int[] counters = new int[2 * Bucket.MAX_BUCKET_SIZE];

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      final int index = i;

      for (int j = 0; j < 12; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.incrementSize(atomicOperation, index));
      }

      counters[i] = 12;
    }

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      final int index = i;
      final int decrement = rnd.nextInt(12);

      for (int j = 0; j < decrement; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.decrementSize(atomicOperation, index));
      }

      counters[i] -= decrement;
    }

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      Assert.assertEquals(counters[i], sizeMap.getSize(i));
    }
  }

  @Test
  public void addDeleteWholePage() throws Exception {
    final long seed = System.nanoTime();
    System.out.printf("addDeleteWholePage seed :%d%n", seed);
    final Random rnd = new Random(seed);

    final Set<Integer> deleted = new HashSet<>();
    final Set<Integer> added = new HashSet<>();

    for (int i = 0; i < 2 * Bucket.MAX_BUCKET_SIZE; i++) {
      Assert.assertTrue(
          added.add(
              atomicOperationsManager.calculateInsideAtomicOperation(
                  null, atomicOperation -> sizeMap.addTree(atomicOperation))));
    }

    final int itemsToDelete = rnd.nextInt(added.size() - 3) + 3;

    for (int i = 0; i < itemsToDelete; i++) {
      int idToDelete;
      do {
        idToDelete = rnd.nextInt(added.size());
      } while (deleted.contains(idToDelete));

      Assert.assertTrue(deleted.add(idToDelete));

      final int ridBagId = idToDelete;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.remove(atomicOperation, ridBagId));
    }

    for (int i = 0; i < itemsToDelete; i++) {
      Assert.assertTrue(
          deleted.remove(
              atomicOperationsManager.calculateInsideAtomicOperation(
                  null, atomicOperation -> sizeMap.addTree(atomicOperation))));
    }
  }

  @Test
  public void addDeletePartialPages() throws Exception {
    final long seed = System.nanoTime();
    System.out.printf("addDeletePartialPages seed :%d%n", seed);
    final Random rnd = new Random(seed);

    final Set<Integer> deleted = new HashSet<>();
    final Set<Integer> added = new HashSet<>();

    final int entries = (int) (Bucket.MAX_BUCKET_SIZE * (rnd.nextDouble() * 1.5 + 0.5));
    for (int i = 0; i < entries; i++) {
      Assert.assertTrue(
          added.add(
              atomicOperationsManager.calculateInsideAtomicOperation(
                  null, atomicOperation -> sizeMap.addTree(atomicOperation))));
    }

    final int entriesToDelete = rnd.nextInt(added.size() - 3) + 3;
    System.out.printf("Total entries %d, entries to delete %d%n", entries, entriesToDelete);

    for (int i = 0; i < entriesToDelete; i++) {
      int idToDelete;
      do {
        idToDelete = rnd.nextInt(added.size());
      } while (deleted.contains(idToDelete));

      Assert.assertTrue(deleted.add(idToDelete));

      final int ridBagId = idToDelete;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.remove(atomicOperation, ridBagId));
    }

    for (int i = 0; i < entriesToDelete; i++) {
      Assert.assertTrue(
          deleted.remove(
              atomicOperationsManager.calculateInsideAtomicOperation(
                  null, atomicOperation -> sizeMap.addTree(atomicOperation))));
    }
  }

  @Test
  public void decrementWithReUsedTrees() throws Exception {
    final long seed = System.nanoTime();
    System.out.printf("decrementWithReUsedTrees seed :%d%n", seed);
    final Random rnd = new Random(seed);

    final Set<Integer> deleted = new HashSet<>();

    final int entries = (int) (Bucket.MAX_BUCKET_SIZE * (rnd.nextDouble() * 1.5 + 0.5));
    for (int i = 0; i < entries; i++) {
      atomicOperationsManager.calculateInsideAtomicOperation(
          null, atomicOperation -> sizeMap.addTree(atomicOperation));
    }

    final int entriesToDelete = rnd.nextInt(entries - 3) + 3;
    System.out.printf("Total entries %d, entries to delete %d%n", entries, entriesToDelete);

    for (int i = 0; i < entriesToDelete; i++) {
      int idToDelete;
      do {
        idToDelete = rnd.nextInt(entries);
      } while (deleted.contains(idToDelete));

      Assert.assertTrue(deleted.add(idToDelete));

      final int ridBagId = idToDelete;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.remove(atomicOperation, ridBagId));
    }

    for (int i = 0; i < entriesToDelete; i++) {
      atomicOperationsManager.calculateInsideAtomicOperation(
          null, atomicOperation -> sizeMap.addTree(atomicOperation));
    }

    final int[] counters = new int[entries];

    for (int i = 0; i < entries; i++) {
      final int index = i;

      for (int j = 0; j < 12; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.incrementSize(atomicOperation, index));
      }

      counters[i] = 12;
    }

    for (int i = 0; i < entries; i++) {
      final int index = i;
      final int decrement = rnd.nextInt(12);

      for (int j = 0; j < decrement; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.decrementSize(atomicOperation, index));
      }

      counters[i] -= decrement;
    }

    for (int i = 0; i < entries; i++) {
      Assert.assertEquals(counters[i], sizeMap.getSize(i));
    }
  }

  @Test
  public void decrementWithDeletedTrees() throws Exception {
    final long seed = 360184606823412L; // System.nanoTime();
    System.out.printf("decrementWithDeletedTrees seed :%d%n", seed);
    final Random rnd = new Random(seed);

    final Set<Integer> deleted = new HashSet<>();
    final Set<Integer> added = new HashSet<>();

    final int entries = (int) (Bucket.MAX_BUCKET_SIZE * (rnd.nextDouble() * 1.5 + 0.5));
    final int[] counters = new int[entries];

    for (int i = 0; i < entries; i++) {
      atomicOperationsManager.calculateInsideAtomicOperation(
          null, atomicOperation -> sizeMap.addTree(atomicOperation));
      added.add(i);
    }

    for (int i = 0; i < entries; i++) {
      final int index = i;

      for (int j = 0; j < 12; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.incrementSize(atomicOperation, index));
      }

      counters[i] = 12;
    }

    final int entriesToDelete = rnd.nextInt(entries - 3) + 3;
    System.out.printf("Total entries %d, entries to delete %d%n", entries, entriesToDelete);

    for (int i = 0; i < entriesToDelete; i++) {
      int idToDelete;
      do {
        idToDelete = rnd.nextInt(entries);
      } while (deleted.contains(idToDelete));

      Assert.assertTrue(deleted.add(idToDelete));
      Assert.assertTrue(added.remove(idToDelete));

      final int ridBagId = idToDelete;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sizeMap.remove(atomicOperation, ridBagId));
    }

    for (int i : added) {
      final int index = i;
      final int decrement = rnd.nextInt(12);

      for (int j = 0; j < decrement; j++) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sizeMap.decrementSize(atomicOperation, index));
      }

      counters[i] -= decrement;
    }

    for (int i : added) {
      Assert.assertEquals(counters[i], sizeMap.getSize(i));
    }
  }
}
