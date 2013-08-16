package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 12.08.13
 */
@Test
public class SBTreeNonLeafBucketTest {
  private final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  public void testInitialization() throws Exception {
    long pointer = directMemory.allocate(OSBTreeBucket.MAX_BUCKET_SIZE_BYTES);

    OSBTreeBucket<Long> treeBucket = new OSBTreeBucket<Long>(pointer, false, OLongSerializer.INSTANCE,
        com.orientechnologies.orient.core.storage.impl.local.paginated.OAbstractPLocalPage.TrackMode.BOTH);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());

    treeBucket = new OSBTreeBucket<Long>(pointer, OLongSerializer.INSTANCE,
        com.orientechnologies.orient.core.storage.impl.local.paginated.OAbstractPLocalPage.TrackMode.BOTH);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling(), -1);
    Assert.assertEquals(treeBucket.getRightSibling(), -1);

    directMemory.free(pointer);
  }

  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    long pointer = directMemory.allocate(OSBTreeBucket.MAX_BUCKET_SIZE_BYTES);
    OSBTreeBucket<Long> treeBucket = new OSBTreeBucket<Long>(pointer, false, OLongSerializer.INSTANCE,
        com.orientechnologies.orient.core.storage.impl.local.paginated.OAbstractPLocalPage.TrackMode.BOTH);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(index,
          new OSBTreeBucket.SBTreeEntry<Long>(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE), key, null),
          true))
        break;

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    long prevRight = -1;
    for (int i = 0; i < treeBucket.size(); i++) {
      OSBTreeBucket.SBTreeEntry<Long> entry = treeBucket.getEntry(i);

      if (prevRight > 0)
        Assert.assertEquals(entry.leftChild, prevRight);

      prevRight = entry.rightChild;
    }

    long prevLeft = -1;
    for (int i = treeBucket.size() - 1; i >= 0; i--) {
      OSBTreeBucket.SBTreeEntry<Long> entry = treeBucket.getEntry(i);

      if (prevLeft > 0)
        Assert.assertEquals(entry.rightChild, prevLeft);

      prevLeft = entry.leftChild;
    }

    directMemory.free(pointer);
  }

  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    long pointer = directMemory.allocate(OSBTreeBucket.MAX_BUCKET_SIZE_BYTES);
    OSBTreeBucket<Long> treeBucket = new OSBTreeBucket<Long>(pointer, false, OLongSerializer.INSTANCE,
        com.orientechnologies.orient.core.storage.impl.local.paginated.OAbstractPLocalPage.TrackMode.BOTH);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long>(index, index + 1, key, null), true))
        break;

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2);
    Assert.assertEquals(treeBucket.size(), index / 2);

    index = 0;
    final Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();

    Iterator<Long> keysIterator = keys.iterator();
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<Long>(keyIndexEntry.getValue(), keyIndexEntry.getValue() + 1,
          keyIndexEntry.getKey(), null));
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long>(index, index + 1, key, null), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<Long>(keyIndexEntry.getValue(), keyIndexEntry.getValue() + 1,
          keyIndexEntry.getKey(), null));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    directMemory.free(pointer);
  }

}
