package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;

/**
 * @author Andrey Lomakin
 * @since 09.08.13
 */
@Test
public class OSBTreeBonsaiLeafBucketTest {
  public void testInitialization() throws Exception {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);

    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());

    treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE,
        ODurablePage.TrackMode.FULL);
    Assert.assertEquals(treeBucket.size(), 0);
    Assert.assertTrue(treeBucket.isLeaf());
    Assert.assertFalse(treeBucket.getLeftSibling().isValid());
    Assert.assertFalse(treeBucket.getRightSibling().isValid());

    pointer.free();
  }

  public void testSearch() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testSearch seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);

    int index = 0;
    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
        break;
      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(treeBucket.size(), keyIndexMap.size());

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      int bucketIndex = treeBucket.find(keyIndexEntry.getKey());
      Assert.assertEquals(bucketIndex, (int) keyIndexEntry.getValue());
    }

    pointer.free();
  }

  public void testUpdateValue() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testUpdateValue seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);

    Map<Long, Integer> keyIndexMap = new HashMap<Long, Integer>();
    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
    }

    Assert.assertEquals(keyIndexMap.size(), treeBucket.size());

    for (int i = 0; i < treeBucket.size(); i++)
      treeBucket.updateValue(i, new ORecordId(i + 5, OClusterPositionFactory.INSTANCE.valueOf(i + 5)));

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, keyIndexEntry.getKey(), new ORecordId(keyIndexEntry.getValue() + 5,
              OClusterPositionFactory.INSTANCE.valueOf(keyIndexEntry.getValue() + 5))));
      Assert.assertEquals(keyIndexEntry.getKey(), treeBucket.getKey(keyIndexEntry.getValue()));
    }

    pointer.free();
  }

  public void testShrink() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testShrink seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
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

      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, keyIndexEntry.getKey(), new ORecordId(keyIndexEntry.getValue(),
              OClusterPositionFactory.INSTANCE.valueOf(keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    pointer.free();
  }

  public void testRemove() throws Exception {
    long seed = System.currentTimeMillis();
    System.out.println("testRemove seed : " + seed);

    TreeSet<Long> keys = new TreeSet<Long>();
    Random random = new Random(seed);

    while (keys.size() < 2 * OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES / OLongSerializer.LONG_SIZE) {
      keys.add(random.nextLong());
    }

    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);

    int index = 0;
    for (Long key : keys) {
      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
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

      if (!treeBucket.addEntry(index, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, key, new ORecordId(index, OClusterPositionFactory.INSTANCE.valueOf(index))), true))
        break;

      keyIndexMap.put(key, index);
      index++;
      addedKeys++;
    }

    for (Map.Entry<Long, Integer> keyIndexEntry : keyIndexMap.entrySet()) {
      OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable> entry = treeBucket.getEntry(keyIndexEntry.getValue());

      Assert.assertEquals(entry, new OSBTreeBonsaiBucket.SBTreeEntry<Long, OIdentifiable>(OBonsaiBucketPointer.NULL,
          OBonsaiBucketPointer.NULL, keyIndexEntry.getKey(), new ORecordId(keyIndexEntry.getValue(),
              OClusterPositionFactory.INSTANCE.valueOf(keyIndexEntry.getValue()))));
    }

    Assert.assertEquals(treeBucket.size(), originalSize);
    Assert.assertEquals(addedKeys, keysToAdd);

    pointer.free();
  }

  public void testSetLeftSibling() throws Exception {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);
    final OBonsaiBucketPointer p = new OBonsaiBucketPointer(123, 8192 * 2);
    treeBucket.setLeftSibling(p);
    Assert.assertEquals(treeBucket.getLeftSibling(), p);

    pointer.free();
  }

  public void testSetRightSibling() throws Exception {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    OSBTreeBonsaiBucket<Long, OIdentifiable> treeBucket = new OSBTreeBonsaiBucket<Long, OIdentifiable>(pointer, 0, true,
        OLongSerializer.INSTANCE, OLinkSerializer.INSTANCE, ODurablePage.TrackMode.FULL);
    final OBonsaiBucketPointer p = new OBonsaiBucketPointer(123, 8192 * 2);
    treeBucket.setRightSibling(p);
    Assert.assertEquals(treeBucket.getRightSibling(), p);

    pointer.free();
  }
}
