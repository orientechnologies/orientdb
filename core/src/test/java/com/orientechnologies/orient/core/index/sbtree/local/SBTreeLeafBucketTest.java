package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 09.08.13
 */
@Test
public class SBTreeLeafBucketTest {
  public void testInitialization() throws Exception {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertEquals(treeBucket.getLeftSibling(), -1);
    Assert.assertEquals(treeBucket.getRightSibling(), -1);

    cachePointer.decrementReferrer();
  }

  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);

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

    cachePointer.decrementReferrer();
  }

  public void testUpdateValue() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    cachePointer.incrementReferrer();

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);

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

    cachePointer.decrementReferrer();
  }

  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);

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

    cachePointer.decrementReferrer();
  }

  public void testRemove() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBucket.MAX_PAGE_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);

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

    cachePointer.decrementReferrer();
  }

  public void testSetLeftSibling() throws Exception {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);
    treeBucket.setLeftSibling(123);
    Assert.assertEquals(treeBucket.getLeftSibling(), 123);

    cachePointer.decrementReferrer();
  }

  public void testSetRightSibling() throws Exception {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OSBTreeBucket.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OSBTreeBucket<Long, OIdentifiable> treeBucket = new OSBTreeBucket<Long, OIdentifiable>(cacheEntry, true,
        OLongSerializer.INSTANCE, null, OLinkSerializer.INSTANCE, null);
    treeBucket.setRightSibling(123);
    Assert.assertEquals(treeBucket.getRightSibling(), 123);

    cachePointer.decrementReferrer();
  }
}
