package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

public class OPaginatedClusterDeleteRecordCOSerializationTest {
  @Test
  public void serializationTest() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    int clusterId = 23;
    long recordPosition = 567;

    OPaginatedClusterDeleteRecordCO paginatedClusterDeleteRecordCO = new OPaginatedClusterDeleteRecordCO(clusterId, recordPosition,
        new byte[] { 1 }, 1, (byte) 1);
    paginatedClusterDeleteRecordCO.setOperationUnitId(operationUnitId);

    final int size = paginatedClusterDeleteRecordCO.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = paginatedClusterDeleteRecordCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    OPaginatedClusterDeleteRecordCO restoredClusterDeleteRecordCO = new OPaginatedClusterDeleteRecordCO();
    pos = restoredClusterDeleteRecordCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredClusterDeleteRecordCO.getOperationUnitId());
    Assert.assertEquals(clusterId, restoredClusterDeleteRecordCO.getClusterId());
    Assert.assertEquals(recordPosition, restoredClusterDeleteRecordCO.getRecordPosition());
  }
}
