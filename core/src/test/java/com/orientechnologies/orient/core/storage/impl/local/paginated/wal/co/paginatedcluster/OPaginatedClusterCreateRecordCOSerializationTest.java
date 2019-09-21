package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OPaginatedClusterCreateRecordCOSerializationTest {
  @Test
  public void serializationTest() {
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int clusterId = 42;

    final byte[] recordContent = new byte[12];
    final Random random = new Random();
    random.nextBytes(recordContent);

    final int recordVersion = 56;
    final byte recordType = 12;
    final long allocatedPosition = 456;
    final long recordPosition = 234;

    OPaginatedClusterCreateRecordCO createRecordCO = new OPaginatedClusterCreateRecordCO(clusterId, recordContent, recordVersion,
        recordType, allocatedPosition, recordPosition);
    createRecordCO.setOperationUnitId(operationUnitId);

    final int size = createRecordCO.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = createRecordCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OPaginatedClusterCreateRecordCO restoredCreateRecordCO = new OPaginatedClusterCreateRecordCO();
    pos = restoredCreateRecordCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCreateRecordCO.getOperationUnitId());
    Assert.assertEquals(recordVersion, restoredCreateRecordCO.getRecordVersion());
    Assert.assertEquals(recordType, restoredCreateRecordCO.getRecordType());
    Assert.assertEquals(allocatedPosition, restoredCreateRecordCO.getAllocatedPosition());
    Assert.assertEquals(recordPosition, restoredCreateRecordCO.getRecordPosition());
  }
}
