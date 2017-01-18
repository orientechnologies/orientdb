package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 09.08.13
 */
public class SBTreeLeafBucketTest {
  @Test
  public void testInitialization() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling(), -1);
    Assert.assertEquals(treeBucket.getRightSibling(), -1);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
        break;
      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testUpdateValue() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool,0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);

    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++)
      treeBucket.updateValue(i, new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(i + 5, i + 5)));

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, keyIndexEntry.getKey(),
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue() + 5, keyIndexEntry.getValue() + 5))));
      Assert.assertEquals(keyIndexEntry.getKey(), treeBucket.getKey(keyIndexEntry.getValue()));
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

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
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

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, keyIndexEntry.getKey(),
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool,0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
        break;

      index++;
    }

    int originalSize = treeBucket.size();

    int itemsToDelete = originalSize / 2;
    for (int i = 0; i < itemsToDelete; i++) {
      treeBucket.remove(treeBucket.size() - 1);
    }

    Assert.assertEquals(treeBucket.size(), originalSize - itemsToDelete);

    final Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    Iterator<Long> keysIterator = keys.iterator();

    index = 0;
    while (keysIterator.hasNext() && index < treeBucket.size()) {
      Long key = keysIterator.next();
      keyIndexMap.put(key, index);
      index++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, key,
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(index, index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(-1, -1, keyIndexEntry.getKey(),
          new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);
    treeBucket.setLeftSibling(123);
    Assert.assertEquals(treeBucket.getLeftSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE);
    treeBucket.setRightSibling(123);
    Assert.assertEquals(treeBucket.getRightSibling(), 123);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
