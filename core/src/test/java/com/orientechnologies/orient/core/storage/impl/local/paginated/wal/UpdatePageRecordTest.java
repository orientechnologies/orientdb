package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 29.04.13
 */
public class UpdatePageRecordTest {
  @Test
  public void testSerializationPrevLSNIsNotNull() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(12, 34));

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(12, 34), restoredUpdatePageRecord.getPrevLsn());
  }

  @Test
  public void testSerializationPrevLSNIsNotNullBuffer() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(12, 34));

    int serializedSize = serializedUpdatePageRecord.serializedSize() + 1;
    ByteBuffer content = ByteBuffer.allocate(serializedSize).order(ByteOrder.nativeOrder());
    content.position(1);

    serializedUpdatePageRecord.toStream(content);
    Assert.assertEquals(serializedSize, content.position());

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content.array(), 1);
    Assert.assertEquals(fromStreamOffset, content.position());

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(12, 34), restoredUpdatePageRecord.getPrevLsn());
  }

  @Test
  public void testSerializationPrevLSNIsNull() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(15, 72));

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(15, 72), restoredUpdatePageRecord.getPrevLsn());
  }

  @Test
  public void testSerializationPrevLSNIsNullBuffer() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(15, 72));

    int serializedSize = serializedUpdatePageRecord.serializedSize() + 1;
    ByteBuffer content = ByteBuffer.allocate(serializedSize).order(ByteOrder.nativeOrder());
    content.position(1);

    serializedUpdatePageRecord.toStream(content);
    Assert.assertEquals(serializedSize, content.position());

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content.array(), 1);
    Assert.assertEquals(content.position(), fromStreamOffset);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(15, 72), restoredUpdatePageRecord.getPrevLsn());
  }
}
