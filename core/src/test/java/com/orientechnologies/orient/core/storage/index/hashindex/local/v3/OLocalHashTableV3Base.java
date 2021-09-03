package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Created by frank on 24/04/2016. */
public abstract class OLocalHashTableV3Base {
  protected static final int KEYS_COUNT = 500000;
  protected OLocalHashTableV3<Integer, String> localHashTable;
  protected OAtomicOperationsManager atomicOperationsManager;

  @Test
  public void testKeyPut() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
      Assert.assertEquals(localHashTable.get(i), String.valueOf(i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(i + " key is absent", localHashTable.get(i), String.valueOf(i));
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(localHashTable.get(i));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<Integer>();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
    }

    for (int key : keys) Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
  }

  @Test
  public void testKeyPutRandomGaussian() throws IOException {
    Set<Integer> keys = new HashSet<>();
    Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
    }

    for (int key : keys) Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
  }

  @Test
  public void testKeyDeleteRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<>();
    long ms = System.currentTimeMillis();
    System.out.println("testKeyDelete : " + ms);
    final Random random = new Random(ms);

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
    }

    for (int key : keys) {
      if (key % 3 == 0)
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> localHashTable.remove(atomicOperation, key));
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
      }
    }
  }

  @Test
  public void testKeyDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    localHashTable.remove(atomicOperation, key), String.valueOf(key)));
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) Assert.assertNull(localHashTable.get(i));
      else Assert.assertEquals(localHashTable.get(i), i + "");
    }
  }

  @Test
  public void testKeyAddDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    localHashTable.remove(atomicOperation, key), String.valueOf(key)));
      }

      if (i % 2 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                localHashTable.put(
                    atomicOperation, KEYS_COUNT + key, String.valueOf(KEYS_COUNT + key)));
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) Assert.assertNull(localHashTable.get(i));
      else Assert.assertEquals(localHashTable.get(i), String.valueOf(i));

      if (i % 2 == 0)
        Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), String.valueOf(KEYS_COUNT + i));
    }
  }

  @Test
  public void testKeyPutRemoveNullKey() throws IOException {
    for (int i = 0; i < 10; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
    }

    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> localHashTable.put(atomicOperation, null, "null"));

    for (int i = 0; i < 10; i++) Assert.assertEquals(localHashTable.get(i), String.valueOf(i));

    Assert.assertEquals(localHashTable.get(null), "null");

    for (int i = 0; i < 5; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              Assert.assertEquals(
                  localHashTable.remove(atomicOperation, key), String.valueOf(key)));
    }

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            Assert.assertEquals(localHashTable.remove(atomicOperation, null), "null"));

    for (int i = 0; i < 5; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> Assert.assertNull(localHashTable.remove(atomicOperation, key)));
    }

    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> Assert.assertNull(localHashTable.remove(atomicOperation, null)));

    for (int i = 0; i < 5; i++) Assert.assertNull(localHashTable.get(i));

    Assert.assertNull(localHashTable.get(null));

    for (int i = 5; i < 10; i++) Assert.assertEquals(localHashTable.get(i), i + "");
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws IOException {
    HashSet<Integer> keys = new HashSet<Integer>();

    Random random = new Random();
    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
      keys.add(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> localHashTable.remove(atomicOperation, key));
      }
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
      }
    }
  }
}
