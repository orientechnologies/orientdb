package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
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
public class SBTreeLeafBucketTest {
  @Test
  public void testInitialization() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      Assert.assertEquals(bucket.size(), 0);
      Assert.assertTrue(bucket.isLeaf());
    };
    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    treeBucket = new OSBTreeBucket<>(cacheEntry, OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);
    checker = (bucket) -> {
      Assert.assertEquals(bucket.size(), 0);
      Assert.assertTrue(bucket.isLeaf());
      Assert.assertEquals(bucket.getLeftSibling(), -1);
      Assert.assertEquals(bucket.getRightSibling(), -1);
    };
    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSearch() {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<>();
    for (Long key : keys) {
      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true))
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
    };
    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testUpdateValue() {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    Map<Long, Integer> keyIndexMap = new HashMap<>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true)) {
        break;
      }

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++) {
      final byte[] rawValue = new byte[OLinkSerializer.RID_SIZE];

      OLinkSerializer.INSTANCE.serializeNativeObject(new ORecordId(i + 5, i + 5), rawValue, 0);
      treeBucket.updateValue(i, rawValue);
    }

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
        final OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

        Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<>(-1, -1, keyIndexEntry.getKey(),
            new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue() + 5, keyIndexEntry.getValue() + 5))));
        Assert.assertEquals(keyIndexEntry.getKey(), bucket.getKey(keyIndexEntry.getValue()));
      }
    };

    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testShrink() {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true))
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

      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    Assert.assertEquals(addedKeys, keysToAdd);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
        OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

        Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<>(-1, -1, keyIndexEntry.getKey(),
            new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
      }

      Assert.assertEquals(bucket.size(), originalSize);
    };

    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testRemove() {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<>();
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

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true))
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

      if (!treeBucket
          .addEntry(index, new OSBTreeBucket.SBTreeEntry<>(-1, -1, key, new OSBTreeValue<>(false, -1, new ORecordId(index, index))),
              true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    Assert.assertEquals(addedKeys, keysToAdd);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> {
      for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
        OSBTreeBucket.SBTreeEntry<Long, OIdentifiable> entry = bucket.getEntry(keyIndexEntry.getValue());

        Assert.assertEquals(entry, new OSBTreeBucket.SBTreeEntry<>(-1, -1, keyIndexEntry.getKey(),
            new OSBTreeValue<OIdentifiable>(false, -1, new ORecordId(keyIndexEntry.getValue(), keyIndexEntry.getValue()))));
      }

      Assert.assertEquals(bucket.size(), originalSize);
    };

    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetLeftSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);
    treeBucket.setLeftSibling(123);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> Assert.assertEquals(bucket.getLeftSibling(), 123);

    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testSetRightSibling() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);
    treeBucket.setRightSibling(123);

    Consumer<OSBTreeBucket<Long, OIdentifiable>> checker = (bucket) -> Assert.assertEquals(bucket.getRightSibling(), 123);

    checker.accept(treeBucket);
    assertSerialization(treeBucket.serializePage(), checker);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  private void assertSerialization(final byte[] serializedPage, Consumer<OSBTreeBucket<Long, OIdentifiable>> consumer) {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(false);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    final OSBTreeBucket<Long, OIdentifiable> restoredBucket = new OSBTreeBucket<>(cacheEntry, true, OLongSerializer.INSTANCE, null,
        OLinkSerializer.INSTANCE, null);
    restoredBucket.deserializePage(serializedPage);

    consumer.accept(restoredBucket);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
