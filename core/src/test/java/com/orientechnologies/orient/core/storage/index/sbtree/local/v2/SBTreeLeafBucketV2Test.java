package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
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
 * @since 09.08.13
 */
public class SBTreeLeafBucketV2Test {
  @Test
  public void testInitialization() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling(), -1);
    Assert.assertEquals(treeBucket.getRightSibling(), -1);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucketV2.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<>();
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index), true))) {
        break;
      }
      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), OLongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testUpdateValue() {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucketV2.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    Map<Long, Integer> keyIndexMap = new HashMap<>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++) {
      final byte[] rawValue = new byte[OLinkSerializer.RID_SIZE];

      OLinkSerializer.INSTANCE.serializeNativeObject(new ORecordId(i + 5, i + 5), rawValue, 0);
      treeBucket.updateValue(i, rawValue, OLongSerializer.LONG_SIZE);
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucketV2.SBTreeEntry<Long, OIdentifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new OSBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new OSBTreeValue<>(
                  false,
                  -1,
                  new ORecordId(keyIndexEntry.getValue() + 5, keyIndexEntry.getValue() + 5))));
      Assert.assertEquals(
          keyIndexEntry.getKey(),
          treeBucket.getKey(keyIndexEntry.getValue(), OLongSerializer.INSTANCE));
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucketV2.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index)))) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE);
    Assert.assertEquals(treeBucket.size(), index / 2);

    index = 0;
    final Map<Long, Integer> keyIndexMap = new HashMap<>();

    Iterator<Long> keysIterator = keys.iterator();
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), OLongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucketV2.SBTreeEntry<Long, OIdentifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new OSBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new OSBTreeValue<>(
                  false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucketV2.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index)))) {
        break;
      }

      index++;
    }

    int originalSize = treeBucket.size();

    int itemsToDelete = originalSize / 2;
    for (int i = 0; i < itemsToDelete; i++) {
      final byte[] rawKey = treeBucket.getRawKey(i, OLongSerializer.INSTANCE);
      final byte[] rawValue =
          treeBucket.getRawValue(i, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      treeBucket.removeLeafEntry(treeBucket.size() - 1, rawKey, rawValue);
    }

    Assert.assertEquals(treeBucket.size(), originalSize - itemsToDelete);

    final Map<Long, Integer> keyIndexMap = new HashMap<>();
    Iterator<Long> keysIterator = keys.iterator();

    index = 0;
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey(), OLongSerializer.INSTANCE);
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addLeafEntry(
          index,
          OLongSerializer.INSTANCE.serializeNativeAsWhole(key),
          OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(index, index)))) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucketV2.SBTreeEntry<Long, OIdentifiable> entry =
          treeBucket.getEntry(
              keyIndexEntry.getValue(), OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      Assert.assertEquals(
          entry,
          new OSBTreeBucketV2.SBTreeEntry<>(
              -1,
              -1,
              keyIndexEntry.getKey(),
              new OSBTreeValue<>(
                  false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    treeBucket.setLeftSibling(123);
    Assert.assertEquals(treeBucket.getLeftSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBucketV2<>(cacheEntry);
    treeBucket.init(true);

    treeBucket.setRightSibling(123);
    Assert.assertEquals(treeBucket.getRightSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
