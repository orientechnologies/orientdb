package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * @author Andrey Lomakin
 * @since 14.01.13
 */
public class LinearHashingTableTest {
  private static final int KEYS_COUNT = 5000;
  public static final int  MAX_SEED   = 3;

  @Test(enabled = false)
  public void testKeyPut() {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      Assert.assertTrue(linearHashingTable.put(position), "i " + i);
      Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "i " + i);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OClusterPosition position = new OClusterPositionLong(i);
      Assert.assertTrue(linearHashingTable.contains(position), i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      final OClusterPosition position = new OClusterPositionLong(i);
      Assert.assertFalse(linearHashingTable.contains(position));
    }
  }

  @Test
  public void testKeyPutRandom() {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;

    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;

    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      for (long key : keys) {
        final OClusterPosition position = new OClusterPositionLong(key);
        Assert.assertTrue(linearHashingTable.contains(position), "" + key);
      }
      i++;
    }
  }

  @Test(enabled = false)
  public void testKeyDelete() {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();

    for (int i = 0; i < KEYS_COUNT; i++) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      linearHashingTable.put(position);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
        Assert.assertEquals(position, linearHashingTable.delete(position.clusterPosition));
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertFalse(linearHashingTable.contains(position));
      } else {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertTrue(linearHashingTable.contains(position));
      }

    }
  }

  @Test
  public void testKeyDeleteRandom() {
    int seed = 0;
    while (seed < MAX_SEED) {
      OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      HashSet<Long> longs = new HashSet<Long>();
      MersenneTwisterFast random = new MersenneTwisterFast(seed);
      for (int i = 0; i < KEYS_COUNT; i++) {
        long key = random.nextLong(Long.MAX_VALUE);
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          longs.add(key);
        }
      }

      for (long key : longs) {
        if (key % 3 == 0) {
          final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
          Assert.assertEquals(position, linearHashingTable.delete(position.clusterPosition));
        }
      }

      for (long key : longs) {
        if (key % 3 == 0) {
          OClusterPosition position = new OClusterPositionLong(key);
          Assert.assertFalse(linearHashingTable.contains(position));
        } else {
          OClusterPosition position = new OClusterPositionLong(key);
          Assert.assertTrue(linearHashingTable.contains(position));
        }
      }
      seed++;
    }
  }

  @Test(enabled = false)
  // not uniformly distributed data is not allowed while statistic was not implemented
  public void testKeyAddDelete() {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();

    for (int i = 0; i < KEYS_COUNT; i++) {
      OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
      Assert.assertTrue(linearHashingTable.put(position));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(i));
        Assert.assertEquals(position, linearHashingTable.delete(position.clusterPosition));
      }

      if (i % 2 == 0) {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(KEYS_COUNT + i));
        Assert.assertTrue(linearHashingTable.put(position));
      }

    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertFalse(linearHashingTable.contains(position));
      } else {
        OClusterPosition position = new OClusterPositionLong(i);
        Assert.assertTrue(linearHashingTable.contains(position));
      }

      if (i % 2 == 0) {
        OClusterPosition position = new OClusterPositionLong(KEYS_COUNT + i);
        Assert.assertTrue(linearHashingTable.contains(position), "i " + (KEYS_COUNT + i));
      }

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

  @Test
  public void testKeyAddDeleteRandom() {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    int seed = 0;
    while (seed < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      List<Long> longs = getUniqueRandomValuesArray(seed, 2 * KEYS_COUNT);

      // add
      for (int i = 0; i < KEYS_COUNT; i++) {
        linearHashingTable.put(new OPhysicalPosition(new OClusterPositionLong(longs.get(i))));
      }

      // remove+add
      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0) {
          OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(longs.get(i)));

          Assert.assertEquals(position, linearHashingTable.delete(position.clusterPosition));
        }

        if (i % 2 == 0) {
          OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(longs.get(i + KEYS_COUNT)));

          Assert.assertTrue(linearHashingTable.put(position));
          Assert.assertTrue(linearHashingTable.contains(new OClusterPositionLong(longs.get(KEYS_COUNT + i))), "i = " + i);
        }
      }

      // check removed ok
      for (int i = 0; i < KEYS_COUNT; i++) {
        if (i % 3 == 0) {
          OClusterPosition position = new OClusterPositionLong(longs.get(i));
          Assert.assertFalse(linearHashingTable.contains(position));
        } else {
          OClusterPosition position = new OClusterPositionLong(longs.get(i));
          Assert.assertTrue(linearHashingTable.contains(position));
        }

        if (i % 2 == 0) {
          OClusterPosition position = new OClusterPositionLong(longs.get(KEYS_COUNT + i));
          Assert.assertTrue(linearHashingTable.contains(position));
        }

      }

      seed++;
    }
  }

  @Test
  public void testNextHaveRightOrder() throws Exception {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;// Long.MIN_VALUE;

    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      Collections.sort(keys);

      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] hashTableEntries = linearHashingTable
          .ceilingEntries(new OClusterPositionLong(0));
      int curPos = 0;
      for (Long key : keys) {
        OClusterPosition lhKey = hashTableEntries[curPos].key;

        Assert.assertEquals(new OClusterPositionLong(key), lhKey, "" + key);
        curPos++;
        if (curPos >= hashTableEntries.length) {
          hashTableEntries = linearHashingTable.higherEntries(hashTableEntries[hashTableEntries.length - 1].key);
          curPos = 0;
        }
      }
      i++;
    }
  }

  @Test
  public void testNextSkipsRecordValid() throws Exception {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;// Long.MIN_VALUE;

    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      Collections.sort(keys);

      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] hashTableEntries = linearHashingTable
          .ceilingEntries(new OClusterPositionLong(keys.get(10)));
      int curPos = 0;
      for (Long key : keys) {
        if (key < keys.get(10)) {
          continue;
        }
        OClusterPosition lhKey = hashTableEntries[curPos].key;
        Assert.assertEquals(new OClusterPositionLong(key), lhKey, "" + key);

        curPos++;
        if (curPos >= hashTableEntries.length) {
          hashTableEntries = linearHashingTable.higherEntries(hashTableEntries[hashTableEntries.length - 1].key);
          curPos = 0;
        }

      }

      i++;
    }
  }

  @Test
  public void testNextHaveRightOrderUsingNextMethod() throws Exception {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;
    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      Collections.sort(keys);

      // test finding is unsuccessful
      for (Long key : keys) {
        OClusterPosition lhKey = linearHashingTable.ceilingEntries(new OClusterPositionLong(key))[0].key;
        Assert.assertEquals(new OClusterPositionLong(key), lhKey, "" + key);
      }

      // test finding is successful
      for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
        Long key = keys.get(j);
        OClusterPosition lhKey = linearHashingTable.higherEntries(new OClusterPositionLong(key))[0].key;
        Assert.assertEquals(new OClusterPositionLong(keys.get(j + 1)), lhKey, "" + j);
      }

      i++;
    }
  }

  @Test
  public void testNextWithRandomIdGeneration() throws Exception {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;// Long.MIN_VALUE;

    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      Collections.sort(keys);
      OClusterPosition currentRecord = new OClusterPositionLong(keys.get(0) / 2 + keys.get(keys.size() - 1) / 2);
      OClusterPosition nextRecord = linearHashingTable.ceilingEntries(currentRecord)[0].key;

      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] prevRecords = linearHashingTable.floorEntries(currentRecord);
      OClusterPosition prevRecord = prevRecords[prevRecords.length - 1].key;

      Assert.assertTrue(prevRecord.compareTo(currentRecord) <= 0);
      Assert.assertTrue(currentRecord.compareTo(nextRecord) <= 0);

      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] nextRecords = linearHashingTable.higherEntries(prevRecord);
      prevRecords = linearHashingTable.lowerEntries(nextRecord);

      Assert.assertEquals(nextRecord, nextRecords[0].key);
      Assert.assertEquals(prevRecord, prevRecords[prevRecords.length - 1].key);

      i++;
    }
  }

  @Test
  public void testNextHaveRightOrderUsingPrevMethod() throws Exception {
    OLinearHashingTable<OClusterPosition, OPhysicalPosition> linearHashingTable;
    MersenneTwisterFast random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;

    while (i < MAX_SEED) {
      linearHashingTable = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();
      random = new MersenneTwisterFast(i);
      keys.clear();

      while (keys.size() < KEYS_COUNT) {
        long key = random.nextLong(Long.MAX_VALUE);

        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionLong(key));
        if (linearHashingTable.put(position)) {
          keys.add(key);
          Assert.assertTrue(linearHashingTable.contains(position.clusterPosition), "key " + key);
        }
      }

      Collections.sort(keys, new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
          return -o1.compareTo(o2);
        }
      });

      // test finding is unsuccessful
      for (Long key : keys) {
        OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] prevRecords = linearHashingTable
            .lowerEntries(new OClusterPositionLong(key + 1));
        OClusterPosition lhKey = prevRecords[prevRecords.length - 1].key;
        Assert.assertEquals(new OClusterPositionLong(key), lhKey, "" + key);
      }

      // test finding is successful
      for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
        Long key = keys.get(j);
        OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] prevRecords = linearHashingTable
            .lowerEntries(new OClusterPositionLong(key));

        OClusterPosition lhKey = prevRecords[prevRecords.length - 1].key;
        Assert.assertEquals(new OClusterPositionLong(keys.get(j + 1)), lhKey, "" + key);
      }

      i++;
    }
  }

  private void swap(long[] data, int firstIndex, int secondIndex) {
    long tmp = data[firstIndex];
    data[firstIndex] = data[secondIndex];
    data[secondIndex] = tmp;
  }
}
