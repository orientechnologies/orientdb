package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
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
public abstract class LocalHashTableV2Base {
  private static final int KEYS_COUNT = 1_000_000;
  LocalHashTableV2<Integer, String> localHashTable;
  protected OAbstractPaginatedStorage storage;

  @Test
  public void testKeyPut() throws IOException {
    doInRollbackLoop(0, KEYS_COUNT, 100, (value, rollback) -> localHashTable.put(value, value + ""));

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(i + " key is absent", localHashTable.get(i), i + "");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(localHashTable.get(i));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<>();
    final Random random = new Random();

    final OAtomicOperationsManager manager = storage.getAtomicOperationsManager();
    while (keys.size() < KEYS_COUNT) {
      final int key = random.nextInt();

      for (int k = 0; k < 2; k++) {
        manager.startAtomicOperation((String) null, false);
        localHashTable.put(key, key + "");
        manager.endAtomicOperation(k == 0);
      }

      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys) {
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws IOException {
    final Set<Integer> keys = new HashSet<>();
    Random random = new Random();

    final OAtomicOperationsManager manager = storage.getAtomicOperationsManager();
    while (keys.size() < KEYS_COUNT) {
      final int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      for (int k = 0; k < 2; k++) {
        manager.startAtomicOperation((String) null, false);
        localHashTable.put(key, key + "");
        manager.endAtomicOperation(k == 0);
      }
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }

    for (final int key : keys) {
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<>();
    long ms = System.currentTimeMillis();
    System.out.println("testKeyDelete : " + ms);
    final Random random = new Random(ms);

    final OAtomicOperationsManager manager = storage.getAtomicOperationsManager();
    while (keys.size() < KEYS_COUNT) {
      final int key = random.nextInt();

      for (int k = 0; k < 2; k++) {
        manager.startAtomicOperation((String) null, false);
        localHashTable.put(key, key + "");
        manager.endAtomicOperation(k == 0);
      }

      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (final int key : keys) {
      if (key % 3 == 0) {
        for (int k = 0; k < 2; k++) {
          manager.startAtomicOperation((String) null, false);
          localHashTable.remove(key);
          manager.endAtomicOperation(k == 0);
        }
      }
    }

    for (final int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  @Test
  public void testKeyDelete() throws IOException {
    doInRollbackLoop(0, KEYS_COUNT, 100, (value, rollback) -> localHashTable.put(value, value + ""));

    doInRollbackLoop(0, KEYS_COUNT, 100, (value, rollback) -> {
      if (value % 3 == 0) {
        Assert.assertEquals(localHashTable.remove(value), "" + value);
      }
    });

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(localHashTable.get(i));
      } else {
        Assert.assertEquals(localHashTable.get(i), i + "");
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws IOException {
    doInRollbackLoop(0, KEYS_COUNT, 100, (value, rollback) -> localHashTable.put(value, value + ""));

    doInRollbackLoop(0, KEYS_COUNT, 100, (value, rollback) -> {
      if (value % 3 == 0) {
        Assert.assertEquals(localHashTable.remove(value), value + "");
      }

      if (value % 2 == 0) {
        localHashTable.put(KEYS_COUNT + value, (KEYS_COUNT + value) + "");
      }
    });

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(localHashTable.get(i));
      } else {
        Assert.assertEquals(localHashTable.get(i), i + "");
      }

      if (i % 2 == 0) {
        Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), "" + (KEYS_COUNT + i));
      }
    }
  }

  @Test
  public void testKeyPutRemoveNullKey() throws IOException {
    doInRollbackLoop(0, 10, 1, (value, rollback) -> localHashTable.put(value, value + ""));

    final OAtomicOperationsManager manager = storage.getAtomicOperationsManager();

    for (int k = 0; k < 2; k++) {
      manager.startAtomicOperation((String) null, false);
      localHashTable.put(null, "null");
      manager.endAtomicOperation(k == 0);
    }

    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(localHashTable.get(i), i + "");
    }

    Assert.assertEquals(localHashTable.get(null), "null");

    doInRollbackLoop(0, 5, 1, (value, rollback) -> Assert.assertEquals(localHashTable.remove(value), value + ""));

    for (int k = 0; k < 2; k++) {
      manager.startAtomicOperation((String) null, false);
      Assert.assertEquals(localHashTable.remove(null), "null");
      manager.endAtomicOperation(k == 0);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertNull(localHashTable.remove(i));
    }

    Assert.assertNull(localHashTable.remove(null));

    for (int i = 0; i < 5; i++) {
      Assert.assertNull(localHashTable.get(i));
    }

    Assert.assertNull(localHashTable.get(null));

    for (int i = 5; i < 10; i++) {
      Assert.assertEquals(localHashTable.get(i), i + "");
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws IOException {
    HashSet<Integer> keys = new HashSet<>();

    final OAtomicOperationsManager manager = storage.getAtomicOperationsManager();
    final Random random = new Random();
    while (keys.size() < KEYS_COUNT) {
      final int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      for (int k = 0; k < 2; k++) {
        manager.startAtomicOperation((String) null, false);
        localHashTable.put(key, key + "");
        manager.endAtomicOperation(k == 0);
      }

      keys.add(key);
    }

    for (final int key : keys) {
      if (key % 3 == 0) {
        for (int k = 0; k < 2; k++) {
          manager.startAtomicOperation((String) null, false);
          localHashTable.remove(key);
          manager.endAtomicOperation(k == 0);
        }
      }
    }

    for (final int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void doInRollbackLoop(final int start, final int end, final int rollbackSlice, final TxCode code) throws IOException {
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    for (int i = start; i < end; i += rollbackSlice) {
      for (int k = 0; k < 2; k++) {
        atomicOperationsManager.startAtomicOperation((String) null, false);

        int counter = 0;
        while (counter < rollbackSlice && i + counter < end) {
          code.execute(i + counter, k == 0);

          counter++;
        }

        atomicOperationsManager.endAtomicOperation(k == 0);
      }
    }
  }

  private interface TxCode {
    void execute(int value, boolean rollback) throws IOException;
  }
}
