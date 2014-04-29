package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/15/14
 */
@Test
public class ONullBucketTest {
  public void testEmptyBucket() {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(1024);
    ONullBucket<String> bucket = new ONullBucket<String>(pointer, ODurablePage.TrackMode.NONE, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());
    pointer.free();
  }

  public void testAddGetValue() throws IOException {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(1024);
    ONullBucket<String> bucket = new ONullBucket<String>(pointer, ODurablePage.TrackMode.NONE, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "test");

    pointer.free();
  }

  public void testAddRemoveValue() throws IOException {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(1024);
    ONullBucket<String> bucket = new ONullBucket<String>(pointer, ODurablePage.TrackMode.NONE, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    pointer.free();
  }

  public void testAddRemoveAddValue() throws IOException {
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(1024);
    ONullBucket<String> bucket = new ONullBucket<String>(pointer, ODurablePage.TrackMode.NONE, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "testOne"));

    treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "testOne");

    pointer.free();
  }

}
