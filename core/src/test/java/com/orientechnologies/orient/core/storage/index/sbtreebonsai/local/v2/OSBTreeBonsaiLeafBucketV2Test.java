package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v2;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
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
 * @since 09.08.13
 */
public class OSBTreeBonsaiLeafBucketV2Test {
  @Test
  public void testInitialization() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, 0, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertFalse(treeBucket.getLeftSibling().isValid());
    Assert.assertFalse(treeBucket.getRightSibling().isValid());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    for (int n = 0; n < 5; n++) {
      final int pageOffset = (n + 1) * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES;

      final OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, true,
          OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

      int index = 0;
      Map<Long, Integer> keyIndexMap = new HashMap<>();
      for (Long key : keys) {
        if (!treeBucket.addEntry(index,
            new OSBTreeBonsaiBucketV2.SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key,
                new ORecordId(index, index)), true))
          break;
        keyIndexMap.put(key, index);
        index++;
      }

      final Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> checker = (bucket) -> {
        Assert.assertEquals(bucket.size(), keyIndexMap.size());

        for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
          int bucketIndex = bucket.find(keyIndexEntry.getKey());
          Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
        }
      };

      checker.accept(treeBucket);
      ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
      treeBucket.serializePage(bf);

      assertSerialization(bf.array(), checker, pageOffset);
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

    while (keys.size() < 2 * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    cachePointer.incrementReferrer();

    for (int n = 0; n < 5; n++) {
      final int pageOffset = (n + 1) * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES;

      OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, true,
          OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

      int index = 0;
      for (Long key : keys) {
        if (!treeBucket.addEntry(index,
            new OSBTreeBonsaiBucketV2.SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key,
                new ORecordId(index, index)), true))
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

      int keysToAdd = originalSize - treeBucket.size();
      int addedKeys = 0;
      while (keysIterator.hasNext() && index < originalSize) {
        Long key = keysIterator.next();

        if (!treeBucket.addEntry(index,
            new OSBTreeBonsaiBucketV2.SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key,
                new ORecordId(index, index)), true))
          break;

        keyIndexMap.put(key, index);
        index++;
        addedKeys++;
      }

      Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> checker = (bucket) -> {
        for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
          OSBTreeBonsaiBucketV2.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

          Assert.assertEquals(entry,
              new OSBTreeBonsaiBucketV2.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL,
                  keyIndexEntry.getKey(), new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue())));
        }

        Assert.assertEquals(bucket.size(), originalSize);
      };

      checker.accept(treeBucket);

      ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
      treeBucket.serializePage(bf);

      assertSerialization(bf.array(), checker, pageOffset);

      Assert.assertEquals(addedKeys, keysToAdd);
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    for (int n = 0; n < 5; n++) {
      final int pageOffset = (n + 1) * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES;

      OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, true,
          OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

      int index = 0;
      for (Long key : keys) {
        if (!treeBucket.addEntry(index,
            new OSBTreeBonsaiBucketV2.SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key,
                new ORecordId(index, index)), true))
          break;

        index++;
      }

      int originalSize = treeBucket.size();

      int itemsToDelete = originalSize / 2;
      for (int i = 0; i < itemsToDelete; i++) {
        treeBucket.remove(treeBucket.size() - 1);
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
        int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
        Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
      }

      int keysToAdd = originalSize - treeBucket.size();
      int addedKeys = 0;
      while (keysIterator.hasNext() && index < originalSize) {
        Long key = keysIterator.next();

        if (!treeBucket.addEntry(index,
            new OSBTreeBonsaiBucketV2.SBTreeEntry<>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key,
                new ORecordId(index, index)), true))
          break;

        keyIndexMap.put(key, index);
        index++;
        addedKeys++;
      }

      final Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> checker = (bucket) -> {
        for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
          OSBTreeBonsaiBucketV2.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

          Assert.assertEquals(entry,
              new OSBTreeBonsaiBucketV2.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL,
                  keyIndexEntry.getKey(), new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue())));
        }

        Assert.assertEquals(bucket.size(), originalSize);
      };

      checker.accept(treeBucket);

      ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
      treeBucket.serializePage(bf);

      assertSerialization(bf.array(), checker, pageOffset);

      Assert.assertEquals(addedKeys, keysToAdd);
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    for (int n = 0; n < 5; n++) {
      final int pageOffset = (n + 1) * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES;

      OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, true,
          OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);
      final OBonsaiBucketPointer p = new OBonsaiBucketPointer(123, 8192 * 2, OSBTreeBonsaiLocalV2.BINARY_VERSION);
      treeBucket.setLeftSibling(p);

      final Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> checker = (bucket) -> Assert
          .assertEquals(bucket.getLeftSibling(), p);

      checker.accept(treeBucket);

      ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
      treeBucket.serializePage(bf);

      assertSerialization(bf.array(), checker, pageOffset);
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    for (int n = 0; n < 5; n++) {
      final int pageOffset = (n + 1) * OSBTreeBonsaiBucketV2.MAX_BUCKET_SIZE_BYTES;

      OSBTreeBonsaiBucketV2<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, true,
          OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);
      final OBonsaiBucketPointer p = new OBonsaiBucketPointer(123, 8192 * 2, OSBTreeBonsaiLocalV2.BINARY_VERSION);
      treeBucket.setRightSibling(p);

      final Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> checker = (bucket) -> Assert
          .assertEquals(bucket.getRightSibling(), p);

      checker.accept(treeBucket);

      ByteBuffer bf = ByteBuffer.allocate(treeBucket.serializedSize()).order(ByteOrder.nativeOrder());
      treeBucket.serializePage(bf);

      assertSerialization(bf.array(), checker, pageOffset);
    }

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  private void assertSerialization(final byte[] serializedPage, Consumer<OSBTreeBonsaiBucketV2<Long, OIdentifiable>> consumer,
      int pageOffset) {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(false, 0);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBonsaiBucketV2<Long, OIdentifiable> restoredBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry);

    restoredBucket.deserializePage(serializedPage);

    restoredBucket = new OSBTreeBonsaiBucketV2<>(cacheEntry, pageOffset, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, null);

    consumer.accept(restoredBucket);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
