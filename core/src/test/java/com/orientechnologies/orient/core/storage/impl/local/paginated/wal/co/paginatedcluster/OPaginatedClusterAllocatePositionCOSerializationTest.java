package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

public class OPaginatedClusterAllocatePositionCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int clusterId = 123;
    final byte recordType = 34;

    OPaginatedClusterAllocatePositionCO co = new OPaginatedClusterAllocatePositionCO(clusterId, recordType);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = co.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    OPaginatedClusterAllocatePositionCO restoredCO = new OPaginatedClusterAllocatePositionCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(clusterId, restoredCO.getClusterId());
    Assert.assertEquals(recordType, restoredCO.getRecordType());
  }
}
