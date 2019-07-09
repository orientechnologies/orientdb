package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OPaginatedClusterUpdateRecordCOSerializationTest {
  @Test
  public void serializationTest() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    int clusterId = 42;
    long clusterPosition = 345;

    byte[] recordContent = new byte[12];

    Random random = new Random();
    random.nextBytes(recordContent);

    int recordVersion = 34;
    byte recordType = 2;

    byte[] oldRecordContent = new byte[17];
    random.nextBytes(oldRecordContent);

    int oldRecordVersion = 33;
    byte oldRecordType = 3;

    OPaginatedClusterUpdateRecordCO clusterUpdateRecordCO = new OPaginatedClusterUpdateRecordCO(clusterId, clusterPosition,
        recordContent, recordVersion, recordType, oldRecordContent, oldRecordVersion, oldRecordType);
    clusterUpdateRecordCO.setOperationUnitId(operationUnitId);

    int size = clusterUpdateRecordCO.serializedSize();
    byte[] stream = new byte[size + 1];
    int pos = clusterUpdateRecordCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OPaginatedClusterUpdateRecordCO restoredUpdateRecordCO = new OPaginatedClusterUpdateRecordCO();
    pos = restoredUpdateRecordCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    Assert.assertEquals(operationUnitId, restoredUpdateRecordCO.getOperationUnitId());

    Assert.assertEquals(clusterId, restoredUpdateRecordCO.getClusterId());
    Assert.assertEquals(clusterPosition, restoredUpdateRecordCO.getClusterPosition());

    Assert.assertArrayEquals(recordContent, restoredUpdateRecordCO.getRecordContent());
    Assert.assertArrayEquals(oldRecordContent, restoredUpdateRecordCO.getOldRecordContent());

    Assert.assertEquals(recordVersion, restoredUpdateRecordCO.getRecordVersion());
    Assert.assertEquals(recordType, restoredUpdateRecordCO.getRecordType());

    Assert.assertEquals(oldRecordVersion, restoredUpdateRecordCO.getOldRecordVersion());
    Assert.assertEquals(oldRecordType, restoredUpdateRecordCO.getOldRecordType());
  }
}
