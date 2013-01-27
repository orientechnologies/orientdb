package com.orientechnologies.orient.core.storage.impl.memory.eh;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 23.01.13
 */
@Test
public class ExtendibleHashingTableTest {
  private static final int KEYS_COUNT = 120000;
  public static final int  MAX_SEED   = 3;

  public void testKeyPut() {
    OExtendibleHashingTable extendibleHashingTable = new OExtendibleHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      Assert.assertTrue(extendibleHashingTable.put(position), "i " + i);
      Assert.assertTrue(extendibleHashingTable.contains(position.clusterPosition), "i " + i);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OClusterPosition position = new OClusterPositionLong(i);
      Assert.assertTrue(extendibleHashingTable.contains(position), i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      final OClusterPosition position = new OClusterPositionLong(i);
      Assert.assertFalse(extendibleHashingTable.contains(position));
    }
  }

  public void testKeyPutRandom() {
    OExtendibleHashingTable extendibleHashingTable;

    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;

    while (i < MAX_SEED) {
      extendibleHashingTable = new OExtendibleHashingTable();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (extendibleHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(extendibleHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      for (long key : keys) {
        final OClusterPosition position = new OClusterPositionLong(key);
        Assert.assertTrue(extendibleHashingTable.contains(position), "" + key);
      }
      i++;
    }
  }

  @Test
  public void testKeyDeleteRandom() {
    int seed = 0;
    while (seed < MAX_SEED) {
      OExtendibleHashingTable extendibleHashingTable = new OExtendibleHashingTable();
      HashSet<Long> longs = new HashSet<Long>();

      MersenneTwisterFast random = new MersenneTwisterFast(seed);
      for (int i = 0; i < KEYS_COUNT; i++) {
        long key = random.nextLong(Long.MAX_VALUE);
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (extendibleHashingTable.put(position)) {
          longs.add(key);
        }
      }

      for (long key : longs) {
        if (key % 3 == 0) {
          final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
          Assert.assertEquals(position, extendibleHashingTable.delete(position.clusterPosition));
        }
      }

      for (long key : longs) {
        if (key % 3 == 0) {
          OClusterPosition position = new OClusterPositionLong(key);
          Assert.assertFalse(extendibleHashingTable.contains(position));
        } else {
          OClusterPosition position = new OClusterPositionLong(key);
          Assert.assertTrue(extendibleHashingTable.contains(position));
        }
      }
      seed++;
    }
  }

  public void testKeyDelete() {
    OExtendibleHashingTable extendibleHashingTable = new OExtendibleHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      extendibleHashingTable.put(position);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
        Assert.assertEquals(position, extendibleHashingTable.delete(position.clusterPosition));
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertFalse(extendibleHashingTable.contains(position));
      } else {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertTrue(extendibleHashingTable.contains(position));
      }
    }
  }

  public void testKeyAddDelete() {
    OExtendibleHashingTable extendibleHashingTable = new OExtendibleHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++) {
      OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      Assert.assertTrue(extendibleHashingTable.put(position));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
        Assert.assertEquals(position, extendibleHashingTable.delete(position.clusterPosition));
      }

      if (i % 2 == 0) {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(KEYS_COUNT + i));
        Assert.assertTrue(extendibleHashingTable.put(position));
      }

    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertFalse(extendibleHashingTable.contains(position));
      } else {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertTrue(extendibleHashingTable.contains(position));
      }

      if (i % 2 == 0) {
        OClusterPosition position = new OClusterPositionLong(KEYS_COUNT + i);
        Assert.assertTrue(extendibleHashingTable.contains(position), "i " + (KEYS_COUNT + i));
      }

    }
  }

  @Test(enabled = false)
  public void testKeyAddDeleteRandom() {
    OExtendibleHashingTable extendibleHashingTable;
    int seed = 0;
    while (seed < MAX_SEED) {
      extendibleHashingTable = new OExtendibleHashingTable();
      List<Long> longs = getUniqueRandomValuesArray(seed, 2 * KEYS_COUNT);

      // add
      for (int i = 0; i < KEYS_COUNT; i++) {
        extendibleHashingTable.put(new OPhysicalPosition(new OClusterPositionLong(longs.get(i))));
      }

      // remove+add
      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0) {
          OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(longs.get(i)));

          Assert.assertEquals(position, extendibleHashingTable.delete(position.clusterPosition));
        }

        if (i % 2 == 0) {
          OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(longs.get(i + KEYS_COUNT)));

          Assert.assertTrue(extendibleHashingTable.put(position));
          Assert.assertTrue(extendibleHashingTable.contains(new OClusterPositionLong(longs.get(KEYS_COUNT + i))), "i = " + i);
        }
      }

      // check removed ok
      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0) {
          OClusterPosition position = new OClusterPositionLong(longs.get(i));
          Assert.assertFalse(extendibleHashingTable.contains(position));
        } else {
          OClusterPosition position = new OClusterPositionLong(longs.get(i));
          Assert.assertTrue(extendibleHashingTable.contains(position));
        }

        if (i % 2 == 0) {
          OClusterPosition position = new OClusterPositionLong(longs.get(KEYS_COUNT + i));
          Assert.assertTrue(extendibleHashingTable.contains(position));
        }

      }

      seed++;
    }
  }

  private List<Long> getUniqueRandomValuesArray(int seed, int size) {
    MersenneTwisterFast random = new MersenneTwisterFast(seed);
    long data[] = new long[size];
    long multiplicand = Long.MAX_VALUE / size;
    int scatter = Math.abs((int) (multiplicand / 4));
    for (int i = 0, dataLength = data.length; i < dataLength; i++) {
      data[i] = Math.abs(i * multiplicand + random.nextInt(scatter) - scatter);
    }

    int max = data.length - 1;

    List<Long> list = new ArrayList<Long>(size);
    while (max > 0) {

      swap(data, max, Math.abs(random.nextInt(max)));
      list.add(data[max--]);
    }
    return list;
  }

  private void swap(long[] data, int firstIndex, int secondIndex) {
    long tmp = data[firstIndex];
    data[firstIndex] = data[secondIndex];
    data[secondIndex] = tmp;
  }

}
