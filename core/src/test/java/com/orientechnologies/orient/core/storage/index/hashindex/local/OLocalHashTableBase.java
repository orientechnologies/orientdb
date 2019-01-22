package com.orientechnologies.orient.core.storage.index.hashindex.local;

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

  @Test
  public void testKeyPut() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
      Assert.assertEquals(localHashTable.get(i), i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertEquals(i + " key is absent", localHashTable.get(i), i + "");
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

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  @Test
  public void testKeyPutRandomGaussian() throws IOException {
    Set<Integer> keys = new HashSet<Integer>();
    Random random = new Random();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), "" + key);
    }

    for (int key : keys)
      Assert.assertEquals(localHashTable.get(key), "" + key);
  }

  @Test
  public void testKeyDeleteRandomUniform() throws IOException {
    final Set<Integer> keys = new HashSet<Integer>();
    long ms = System.currentTimeMillis();
    System.out.println("testKeyDelete : " + ms);
    final Random random = new Random(ms);

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      localHashTable.put(key, key + "");
      keys.add(key);
      Assert.assertEquals(localHashTable.get(key), key + "");
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }

  @Test
  public void testKeyDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      localHashTable.put(i, i + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), "" + i);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");
    }
  }

  @Test
  public void testKeyAddDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++)
      localHashTable.put(i, i + "");

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertEquals(localHashTable.remove(i), i + "");

      if (i % 2 == 0)
        localHashTable.put(KEYS_COUNT + i, (KEYS_COUNT + i) + "");
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localHashTable.get(i));
      else
        Assert.assertEquals(localHashTable.get(i), i + "");

      if (i % 2 == 0)
        Assert.assertEquals(localHashTable.get(KEYS_COUNT + i), "" + (KEYS_COUNT + i));
    }
  }

  @Test
  public void testKeyPutRemoveNullKey() throws IOException {
    for (int i = 0; i < 10; i++)
      localHashTable.put(i, i + "");

    localHashTable.put(null, "null");

    for (int i = 0; i < 10; i++)
      Assert.assertEquals(localHashTable.get(i), i + "");

    Assert.assertEquals(localHashTable.get(null), "null");

    for (int i = 0; i < 5; i++)
      Assert.assertEquals(localHashTable.remove(i), i + "");

    Assert.assertEquals(localHashTable.remove(null), "null");

    for (int i = 0; i < 5; i++)
      Assert.assertNull(localHashTable.remove(i));

    Assert.assertNull(localHashTable.remove(null));

    for (int i = 0; i < 5; i++)
      Assert.assertNull(localHashTable.get(i));

    Assert.assertNull(localHashTable.get(null));

    for (int i = 5; i < 10; i++)
      Assert.assertEquals(localHashTable.get(i), i + "");
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws IOException {
    HashSet<Integer> keys = new HashSet<Integer>();

    Random random = new Random();
    while (keys.size() < KEYS_COUNT) {
      int key = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);

      localHashTable.put(key, key + "");
      keys.add(key);
    }

    for (int key : keys) {
      if (key % 3 == 0)
        localHashTable.remove(key);
    }

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(localHashTable.get(key));
      } else {
        Assert.assertEquals(localHashTable.get(key), key + "");
      }
    }
  }
}
