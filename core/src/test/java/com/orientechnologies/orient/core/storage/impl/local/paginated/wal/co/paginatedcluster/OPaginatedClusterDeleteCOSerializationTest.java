package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

public class OPaginatedClusterDeleteCOSerializationTest {
  @Test
  public void serializationTest() {
    final String clusterName = "clusterOne";
    final int clusterId = 123;

    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    OPaginatedClusterDeleteCO clusterDeleteCO = new OPaginatedClusterDeleteCO(clusterName, clusterId);
    clusterDeleteCO.setOperationUnitId(operationUnitId);

    final int serializedSize = clusterDeleteCO.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = clusterDeleteCO.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    OPaginatedClusterDeleteCO restoredClusterDeleteCO = new OPaginatedClusterDeleteCO();
    pos = restoredClusterDeleteCO.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    Assert.assertEquals(operationUnitId, restoredClusterDeleteCO.getOperationUnitId());
    Assert.assertEquals(clusterName, restoredClusterDeleteCO.getClusterName());
    Assert.assertEquals(clusterId, restoredClusterDeleteCO.getClusterId());
  }
}
