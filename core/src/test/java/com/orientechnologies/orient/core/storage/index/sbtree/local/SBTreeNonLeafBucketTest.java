package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.08.13
 */
public class SBTreeNonLeafBucketTest {
  @Test
  public void testInitialization() {
    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    final ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();
    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, false, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      Assert.assertEquals(bucket.size(), 0);
      Assert.assertFalse(bucket.isLeaf());
    };

    checker.accept(treeBucket);

    ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
    treeBucket.serializePage(bf);

    assertSerialization(bf.array(), checker);

    treeBucket = new OSBTreeBucket<>(cacheEntry, OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);

    checker = (bucket) -> {
      Assert.assertEquals(bucket.size(), 0);
      Assert.assertFalse(bucket.isLeaf());
      Assert.assertEquals(bucket.getLeftSibling(), -1);
      Assert.assertEquals(bucket.getRightSibling(), -1);
    };

    checker.accept(treeBucket);

    bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
    treeBucket.serializePage(bf);

    assertSerialization(bf.array(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.PAGE_SIZE / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    final ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();
    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, false, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(index,
          new OSBTreeBucket.SBTreeEntry<>(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE), key, null), true))
        break;

      keyIndexMap.put(key, index);
      index++;
    }

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      Assert.assertEquals(bucket.size(), keyIndexMap.size());

      for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
        int bucketIndex = bucket.find(keyIndexEntry.getKey());
        Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
      }

      long prevRight = -1;
      for (int i = 0; i < bucket.size(); i++) {
        OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(i);

        if (prevRight > 0) {
          Assert.assertEquals(entry.leftChild, prevRight);
        }

        prevRight = entry.rightChild;
      }

      long prevLeft = -1;
      for (int i = bucket.size() - 1; i >= 0; i--) {
        OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(i);

        if (prevLeft > 0)
          Assert.assertEquals(entry.rightChild, prevLeft);

        prevLeft = entry.leftChild;
      }
    };
    checker.accept(treeBucket);

    ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
    treeBucket.serializePage(bf);

    assertSerialization(bf.array(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.PAGE_SIZE / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    final ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, false, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<>(index, index + 1, key, null), true))
        break;

      index++;
    }

    int originalSize = treeBucket.size();

    treeBucket.shrink(treeBucket.size() / 2);
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
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry,
          new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(keyIndexEntry.getValue(), keyIndexEntry.getValue() + 1,
              keyIndexEntry.getKey(), null));
    }

    int keysToAdd = originalSize - treeBucket.size();
    int addedKeys = 0;
    while (keysIterator.hasNext() && index < originalSize) {
      Long key = keysIterator.next();

      if (!treeBucket.addEntry(index, new OSBTreeBucket.SBTreeEntry<>(index, index + 1, key, null), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
        OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

        Assert.assertEquals(entry,
            new OSBTreeBucket.SBTreeEntry<Long, OIdentifiable>(keyIndexEntry.getValue(), keyIndexEntry.getValue() + 1,
                keyIndexEntry.getKey(), null));
      }

      Assert.assertEquals(bucket.size(), originalSize);
    };

    checker.accept(treeBucket);

    ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
    treeBucket.serializePage(bf);

    assertSerialization(bf.array(), checker);

    Assert.assertEquals(addedKeys, keysToAdd);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  private void assertSerialization(final byte[] serializedPage, Consumer<OSBTreeBucket<Long, OIdentifiable>> consumer) {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(false);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    final OSBTreeBucket<Long, OIdentifiable> restoredBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);
    restoredBucket.deserializePage(serializedPage);

    consumer.accept(restoredBucket);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

}
