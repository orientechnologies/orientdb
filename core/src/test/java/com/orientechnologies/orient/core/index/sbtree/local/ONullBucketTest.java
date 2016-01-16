package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 4/15/14
 */
@Test
public class ONullBucketTest {
  public void testEmptyBucket() {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(1024);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, null, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());

    cachePointer.decrementReferrer();
  }

  public void testAddGetValue() throws IOException {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(1024);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, null, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "test");

    cachePointer.decrementReferrer();
  }

  public void testAddRemoveValue() throws IOException {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(1024);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, null, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    cachePointer.decrementReferrer();
  }

  public void testAddRemoveAddValue() throws IOException {
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(1024);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, null, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "testOne"));

    treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cachePointer.decrementReferrer();
  }

}
