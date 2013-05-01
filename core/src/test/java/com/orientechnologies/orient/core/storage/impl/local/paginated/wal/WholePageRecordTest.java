package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.Random;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPage;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
@Test
public class WholePageRecordTest {
  public void testSerialization() {
    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    byte[] pageContent = new byte[OLocalPage.PAGE_SIZE];

    Random rnd = new Random();
    rnd.nextBytes(pageContent);

    long pointer = directMemory.allocate(pageContent);
    OWholePageRecord originalPageRecord = new OWholePageRecord(23, "test", pointer);

    byte content[] = new byte[1 + originalPageRecord.serializedSize()];
    Assert.assertEquals(originalPageRecord.toStream(content, 1), content.length);

    OWholePageRecord storedPageRecord = new OWholePageRecord();
    Assert.assertEquals(storedPageRecord.fromStream(content, 1), content.length);

    Assert.assertEquals(storedPageRecord, originalPageRecord);
    directMemory.free(pointer);
  }
}
