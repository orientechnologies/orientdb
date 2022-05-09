package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.08.13
 */
public class OSBTreeBonsaiNonLeafBucketTest {

  @Test
  public void testInitialization() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket =
        new OSBTreeBonsaiBucket<Long, OIdentifiable>(
            cacheEntry, 0, false, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());

    treeBucket =
        new OSBTreeBonsaiBucket<Long, OIdentifiable>(
            cacheEntry, 0, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertFalse(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling().getPageIndex(), -1);
    Assert.assertEquals(treeBucket.getRightSibling().getPageIndex(), -1);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size()
        < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket =
        new OSBTreeBonsaiBucket<Long, OIdentifiable>(
            cacheEntry, 0, false, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(
              new OBonsaiBucketPointer(random.nextInt(Integer.MAX_VALUE), 8192 * 2),
              new OBonsaiBucketPointer(random.nextInt(Integer.MAX_VALUE), 8192 * 2),
              key,
              null),
          true)) break;

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    OBonsaiBucketPointer prevRight = OBonsaiBucketPointer.NULL;
    for (int i = 0; i < treeBucket.size(); i++) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(i);

      if (prevRight.getPageIndex() > 0) Assert.assertEquals(entry.leftChild, prevRight);

      prevRight = entry.rightChild;
    }

    OBonsaiBucketPointer prevLeft = OBonsaiBucketPointer.NULL;
    for (int i = treeBucket.size() - 1; i >= 0; i--) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(i);

      if (prevLeft.getPageIndex() > 0) Assert.assertEquals(entry.rightChild, prevLeft);

      prevLeft = entry.leftChild;
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size()
        < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket =
        new OSBTreeBonsaiBucket<Long, OIdentifiable>(
            cacheEntry, 0, false, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(
          index,
          new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(
              new OBonsaiBucketPointer(index, 8192 * 2),
              new OBonsaiBucketPointer(index + 1, 8192 * 2),
              key,
              null),
          true)) break;

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
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry =
          treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(
          entry,
          new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(
              new OBonsaiBucketPointer(keyIndexEntry.getValue(), 8192 * 2),
              new OBonsaiBucketPointer(keyIndexEntry.getValue() + 1, 8192 * 2),
              keyIndexEntry.getKey(),
              null));
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(
          index,
          new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(
              new OBonsaiBucketPointer(index, 8192 * 2),
              new OBonsaiBucketPointer(index + 1, 8192 * 2),
              key,
              null),
          true)) break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry =
          treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(
          entry,
          new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(
              new OBonsaiBucketPointer(keyIndexEntry.getValue(), 8192 * 2),
              new OBonsaiBucketPointer(keyIndexEntry.getValue() + 1, 8192 * 2),
              keyIndexEntry.getKey(),
              null));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
