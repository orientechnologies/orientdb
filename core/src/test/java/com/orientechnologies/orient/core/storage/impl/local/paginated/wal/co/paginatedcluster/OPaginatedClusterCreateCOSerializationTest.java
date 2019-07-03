package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

public class OPaginatedClusterCreateCOSerializationTest {
  @Test
  public void testSerialization() {
    final String clusterName = "cluster1";
    final int clusterId = 23;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final OPaginatedClusterCreateCO clusterCreateCO = new OPaginatedClusterCreateCO(clusterName, clusterId);
    clusterCreateCO.setOperationUnitId(operationUnitId);

    final int size = clusterCreateCO.serializedSize();

    final byte[] stream = new byte[size + 1];
    int pos = clusterCreateCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    final OPaginatedClusterCreateCO restoredClusterCreateCO = new OPaginatedClusterCreateCO();
    pos = restoredClusterCreateCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredClusterCreateCO.getOperationUnitId());
    Assert.assertEquals(clusterName, restoredClusterCreateCO.getClusterName());
    Assert.assertEquals(clusterId, restoredClusterCreateCO.getClusterId());
  }

}
