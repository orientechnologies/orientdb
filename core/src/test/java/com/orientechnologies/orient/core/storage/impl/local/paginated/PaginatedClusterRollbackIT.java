package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class PaginatedClusterRollbackIT {
  public static  String buildDirectory;
  private static String orientDirectory;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    orientDirectory = buildDirectory + File.separator + PaginatedClusterRollbackIT.class.getName();
    OFileUtils.deleteRecursively(new File(orientDirectory));
  }

  @AfterClass
  public static void afterClass() {
    OFileUtils.deleteRecursively(new File(orientDirectory));
  }

  @Test
  public void testAllocationRollback() throws Exception {
    final String dbName = "testAllocationRollback";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("allocation");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final OPhysicalPosition rolledBackPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        rolledBackPhysicalPosition = cluster.allocatePosition((byte) 2);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        final OPhysicalPosition allocatedPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        allocatedPhysicalPosition = cluster.allocatePosition((byte) 2);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        Assert.assertEquals(rolledBackPhysicalPosition.clusterPosition, allocatedPhysicalPosition.clusterPosition);
      }
    }
  }

  @Test
  public void testAllocationRollbackCamelCase() throws Exception {
    final String dbName = "testAllocationRollbackCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("AlloCation");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final OPhysicalPosition rolledBackPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        rolledBackPhysicalPosition = cluster.allocatePosition((byte) 2);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        final OPhysicalPosition allocatedPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        allocatedPhysicalPosition = cluster.allocatePosition((byte) 2);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        Assert.assertEquals(rolledBackPhysicalPosition.clusterPosition, allocatedPhysicalPosition.clusterPosition);
      }
    }
  }

  @Test
  public void testCreateFromScratch() throws Exception {
    final String dbName = "testCreateFromScratch";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("creation");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition rolledBackPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        rolledBackPhysicalPosition = cluster.createRecord(record, version, recordType, null);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(rolledBackPhysicalPosition.clusterPosition, false);
        Assert.assertNull(rawBuffer);

        final OPhysicalPosition allocatedPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        allocatedPhysicalPosition = cluster.createRecord(record, version, recordType, null);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        Assert.assertEquals(rolledBackPhysicalPosition.clusterPosition, allocatedPhysicalPosition.clusterPosition);
        rawBuffer = cluster.readRecord(rolledBackPhysicalPosition.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);
      }
    }
  }

  @Test
  public void testCreateFromScratchCamelCase() throws Exception {
    final String dbName = "testCreateFromScratchCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("CreaTion");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition rolledBackPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        rolledBackPhysicalPosition = cluster.createRecord(record, version, recordType, null);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(rolledBackPhysicalPosition.clusterPosition, false);
        Assert.assertNull(rawBuffer);

        final OPhysicalPosition allocatedPhysicalPosition;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        allocatedPhysicalPosition = cluster.createRecord(record, version, recordType, null);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        Assert.assertEquals(rolledBackPhysicalPosition.clusterPosition, allocatedPhysicalPosition.clusterPosition);
        rawBuffer = cluster.readRecord(rolledBackPhysicalPosition.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);
      }
    }
  }

  @Test
  public void testUpdateRecord() throws Exception {
    final String dbName = "testUpdateRecord";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("update");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition position = cluster.createRecord(record, version, recordType, null);

        final byte[] updatedRecord = new byte[36];
        random.nextBytes(updatedRecord);

        final int updatedVersion = 3;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.updateRecord(position.clusterPosition, updatedRecord, updatedVersion, recordType);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.updateRecord(position.clusterPosition, updatedRecord, updatedVersion, recordType);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(updatedRecord, rawBuffer.buffer);
        Assert.assertEquals(updatedVersion, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);
      }
    }
  }

  @Test
  public void testUpdateRecordCamelCase() throws Exception {
    final String dbName = "testUpdateRecordCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("UpdAte");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition position = cluster.createRecord(record, version, recordType, null);

        final byte[] updatedRecord = new byte[36];
        random.nextBytes(updatedRecord);

        final int updatedVersion = 3;
        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.updateRecord(position.clusterPosition, updatedRecord, updatedVersion, recordType);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.updateRecord(position.clusterPosition, updatedRecord, updatedVersion, recordType);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(updatedRecord, rawBuffer.buffer);
        Assert.assertEquals(updatedVersion, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);
      }
    }
  }

  @Test
  public void testDeleteRecord() throws Exception {
    final String dbName = "testDeleteRecord";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("delete");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition position = cluster.createRecord(record, version, recordType, null);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.deleteRecord(position.clusterPosition);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.deleteRecord(position.clusterPosition);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        rawBuffer = cluster.readRecord(position.clusterPosition, false);
        Assert.assertNull(rawBuffer);
      }
    }
  }

  @Test
  public void testDeleteRecordCamelCase() throws Exception {
    final String dbName = "testDeleteRecordCamelCase";

    try (OrientDB orientDB = new OrientDB("plocal:" + orientDirectory, OrientDBConfig.defaultConfig())) {
      orientDB.create(dbName, ODatabaseType.PLOCAL);

      try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
        final int clusterId = session.addCluster("DeleTe");
        final OLocalPaginatedStorage storage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
        final OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(clusterId);

        final byte[] record = new byte[25];
        final Random random = new Random();
        random.nextBytes(record);

        final int version = 2;
        final byte recordType = 1;

        final OPhysicalPosition position = cluster.createRecord(record, version, recordType, null);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.deleteRecord(position.clusterPosition);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);

        ORawBuffer rawBuffer = cluster.readRecord(position.clusterPosition, false);

        Assert.assertArrayEquals(record, rawBuffer.buffer);
        Assert.assertEquals(version, rawBuffer.version);
        Assert.assertEquals(recordType, rawBuffer.recordType);

        storage.getAtomicOperationsManager().startAtomicOperation((String) null, false);
        cluster.deleteRecord(position.clusterPosition);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);

        rawBuffer = cluster.readRecord(position.clusterPosition, false);
        Assert.assertNull(rawBuffer);
      }
    }
  }

}
