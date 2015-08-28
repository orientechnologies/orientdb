package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.version.OSimpleVersion;

public class ClusterDebugInfoTest {

  @Test
  public void testOnePageRecordDebugInfo() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ClusterDebugInfoTest");
    db.create();
    try {
      OStorage storage = db.getStorage();
      int defaultId = storage.getDefaultClusterId();
      OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(defaultId);
      int size = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();
      int half = size / 2;
      byte[] content = new byte[half * 1024];
      OStorageOperationResult<OPhysicalPosition> result = storage.createRecord(new ORecordId(defaultId), content,
          new OSimpleVersion(), (byte) 'b', OPERATION_MODE.SYNCHRONOUS.ordinal(), null);

      OPaginatedClusterDebug debug = cluster.readDebug(result.getResult().clusterPosition);

      Assert.assertEquals(debug.clusterPosition, result.getResult().clusterPosition);
      Assert.assertNotEquals(debug.contentSize, 0);
      Assert.assertFalse(debug.empty);
      Assert.assertEquals(debug.pages.size(), 1);

      Assert.assertNotEquals(debug.pages.get(0).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(0).content);
    } finally {
      db.drop();
    }
  }

  @Test
  public void testTwoPagesRecordDebugInfo() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ClusterDebugInfoTest");
    db.create();
    try {
      OStorage storage = db.getStorage();
      int defaultId = storage.getDefaultClusterId();
      OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(defaultId);
      int size = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();
      int half = size / 2;
      byte[] content = new byte[(size + half) * 1024];
      OStorageOperationResult<OPhysicalPosition> result = storage.createRecord(new ORecordId(defaultId), content,
          new OSimpleVersion(), (byte) 'b', OPERATION_MODE.SYNCHRONOUS.ordinal(), null);

      OPaginatedClusterDebug debug = cluster.readDebug(result.getResult().clusterPosition);

      Assert.assertEquals(debug.pages.size(), 2);
      Assert.assertEquals(debug.clusterPosition, result.getResult().clusterPosition);
      Assert.assertNotEquals(debug.contentSize, 0);
      Assert.assertFalse(debug.empty);

      Assert.assertNotEquals(debug.pages.get(0).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(0).content);

      Assert.assertNotEquals(debug.pages.get(1).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(1).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(1).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(1).content);

    } finally {
      db.drop();
    }
  }

  @Test
  public void testThreePagesRecordDebugInfo() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ClusterDebugInfoTest");
    db.create();
    try {
      OStorage storage = db.getStorage();
      int defaultId = storage.getDefaultClusterId();
      OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(defaultId);
      int size = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();
      byte[] content = new byte[(size + size) * 1024];
      OStorageOperationResult<OPhysicalPosition> result = storage.createRecord(new ORecordId(defaultId), content,
          new OSimpleVersion(), (byte) 'b', OPERATION_MODE.SYNCHRONOUS.ordinal(), null);
      OPaginatedClusterDebug debug = cluster.readDebug(result.getResult().clusterPosition);

      Assert.assertEquals(debug.clusterPosition, result.getResult().clusterPosition);
      Assert.assertNotEquals(debug.contentSize, 0);
      Assert.assertFalse(debug.empty);
      Assert.assertEquals(debug.pages.size(), 3);

      Assert.assertNotEquals(debug.pages.get(0).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(0).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(0).content);

      Assert.assertNotEquals(debug.pages.get(1).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(1).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(1).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(1).content);

      Assert.assertNotEquals(debug.pages.get(2).pageIndex, -1);
      Assert.assertNotEquals(debug.pages.get(2).inPagePosition, -1);
      Assert.assertNotEquals(debug.pages.get(2).inPageSize, -1);
      Assert.assertNotNull(debug.pages.get(2).content);
    } finally {
      db.drop();
    }
  }

}
