package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
@Test
public class DirtyPagesRecordTest {
  public void testSerialization() {
    // Temporary fix there is some other test that leave the db open.
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    Set<ODirtyPage> dirtyPages = new HashSet<ODirtyPage>();
    Random rnd = new Random();
    for (int i = 0; i < 10; i++) {
      long pagIndex = rnd.nextLong();
      long position = rnd.nextLong();
      if (position < 0)
        position = -position;

      int segment = rnd.nextInt();
      if (segment < 0)
        segment = -segment;

      dirtyPages.add(new ODirtyPage("test", pagIndex, new OLogSequenceNumber(segment, position)));
    }

    ODirtyPagesRecord originalDirtyPagesRecord = new ODirtyPagesRecord(dirtyPages);

    byte[] content = new byte[originalDirtyPagesRecord.serializedSize() + 1];
    Assert.assertEquals(originalDirtyPagesRecord.toStream(content, 1), content.length);

    ODirtyPagesRecord storedDirtyPagesRecord = new ODirtyPagesRecord();
    Assert.assertEquals(storedDirtyPagesRecord.fromStream(content, 1), content.length);

    Assert.assertEquals(storedDirtyPagesRecord, originalDirtyPagesRecord);
  }
}
