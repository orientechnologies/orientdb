package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by frank on 24/04/2016.
 */
public abstract class OLocalHashTableBase {
  protected static final int                              KEYS_COUNT = 500000;
  protected              OLocalHashTable<Integer, String> localHashTable;
  protected              OAtomicOperationsManager         atomicOperationsManager;

  @Test
  public void testKeyPut() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      for (int i = 0; i < KEYS_COUNT; i++) {
        localHashTable.put(atomicOperation, i, i + "");
        Assert.assertEquals(localHashTable.get(i), i + "");
      }

      for (int i = 0; i < KEYS_COUNT; i++) {
        Assert.assertEquals(i + " key is absent", localHashTable.get(i), i + "");
      }

      for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
        Assert.assertNull(localHashTable.get(i));
      }
    });
  }

  @Test
  public void testKeyPutRandomUniform() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      final Set<Integer> keys = new HashSet<Integer>();
      final Random random = new Random();

      while (keys.size() < KEYS_COUNT) {
        int key = random.nextInt();

        localHashTable.put(atomicOperation, key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), key + "");
      }

      for (int key : keys)
        Assert.assertEquals(localHashTable.get(key), "" + key);
    });
  }

  @Test
  public void testKeyPutRandomGaussian() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      Set<Integer> keys = new HashSet<Integer>();
      Random random = new Random();
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

        localHashTable.put(atomicOperation, key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), "" + key);
      }

      for (int key : keys)
        Assert.assertEquals(localHashTable.get(key), "" + key);
    });
  }

  @Test
  public void testKeyDeleteRandomUniform() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      final Set<Integer> keys = new HashSet<Integer>();
      long ms = System.currentTimeMillis();
      System.out.println("testKeyDelete : " + ms);
      final Random random = new Random(ms);

      while (keys.size() < KEYS_COUNT) {
        int key = random.nextInt();

        localHashTable.put(atomicOperation, key, key + "");
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), key + "");
      }

      for (int key : keys) {
        if (key % 3 == 0)
          localHashTable.remove(atomicOperation, key);
      }

      for (int key : keys) {
        if (key % 3 == 0) {
          Assert.assertNull(localHashTable.get(key));
        } else {
          Assert.assertEquals(localHashTable.get(key), key + "");
        }
      }
    });
  }

  @Test
  public void testKeyDelete() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      for (int i = 0; i < KEYS_COUNT; i++) {
        localHashTable.put(atomicOperation, i, i + "");
      }

      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0)
          Assert.assertEquals(localHashTable.remove(atomicOperation, i), "" + i);
      }

      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0)
          Assert.assertNull(localHashTable.get(i));
        else
          Assert.assertEquals(localHashTable.get(i), i + "");
      }
    });
  }

  @Test
  public void testKeyAddDelete() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      for (int i = 0; i < KEYS_COUNT; i++)
        localHashTable.put(atomicOperation, i, i + "");

      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0)
          Assert.assertEquals(localHashTable.remove(atomicOperation, i), i + "");

        if (i % 2 == 0)
          localHashTable.put(atomicOperation, KEYS_COUNT + i, (KEYS_COUNT + i) + "");
      }

      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0)
          Assert.assertNull(localHashTable.get(i));
        else
          Assert.assertEquals(localHashTable.get(i), i + "");

        if (i % 2 == 0)
          Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), "" + (KEYS_COUNT + i));
      }
    });
  }

  @Test
  public void testKeyPutRemoveNullKey() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      for (int i = 0; i < 10; i++)
        localHashTable.put(atomicOperation, i, i + "");

      localHashTable.put(atomicOperation, null, "null");

      for (int i = 0; i < 10; i++)
        Assert.assertEquals(localHashTable.get(i), i + "");

      Assert.assertEquals(localHashTable.get(null), "null");

      for (int i = 0; i < 5; i++)
        Assert.assertEquals(localHashTable.remove(atomicOperation, i), i + "");

      Assert.assertEquals(localHashTable.remove(atomicOperation, null), "null");

      for (int i = 0; i < 5; i++)
        Assert.assertNull(localHashTable.remove(atomicOperation, i));

      Assert.assertNull(localHashTable.remove(atomicOperation, null));

      for (int i = 0; i < 5; i++)
        Assert.assertNull(localHashTable.get(i));

      Assert.assertNull(localHashTable.get(null));

      for (int i = 5; i < 10; i++)
        Assert.assertEquals(localHashTable.get(i), i + "");
    });
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation((atomicOperation) -> {
      HashSet<Integer> keys = new HashSet<Integer>();

      Random random = new Random();
      while (keys.size() < KEYS_COUNT) {
        int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

        localHashTable.put(atomicOperation, key, key + "");
        keys.add(key);
      }

      for (int key : keys) {
        if (key % 3 == 0)
          localHashTable.remove(atomicOperation, key);
      }

      for (int key : keys) {
        if (key % 3 == 0) {
          Assert.assertNull(localHashTable.get(key));
        } else {
          Assert.assertEquals(localHashTable.get(key), key + "");
        }
      }
    });
  }
}
