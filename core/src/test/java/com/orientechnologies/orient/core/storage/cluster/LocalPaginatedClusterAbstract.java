package com.orientechnologies.orient.core.storage.cluster;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class LocalPaginatedClusterAbstract {
  protected static String buildDirectory;
  protected static OPaginatedCluster paginatedCluster;
  protected static ODatabaseDocumentInternal databaseDocumentTx;
  protected static OrientDB orientDB;
  protected static String dbName;
  protected static OAbstractPaginatedStorage storage;
  private static OAtomicOperationsManager atomicOperationsManager;

  @AfterClass
  public static void afterClass() throws IOException {
    final long firstPosition = paginatedCluster.getFirstPosition();
    OPhysicalPosition[] positions =
        paginatedCluster.ceilingPositions(new OPhysicalPosition(firstPosition));
    while (positions.length > 0) {
      for (OPhysicalPosition position : positions) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.deleteRecord(atomicOperation, position.clusterPosition));
      }
      positions = paginatedCluster.higherPositions(positions[positions.length - 1]);
    }
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> paginatedCluster.delete(atomicOperation));

    orientDB.drop(dbName);
    orientDB.close();
  }

  @Before
  public void beforeMethod() throws IOException {
    atomicOperationsManager = storage.getAtomicOperationsManager();
    final long firstPosition = paginatedCluster.getFirstPosition();
    OPhysicalPosition[] positions =
        paginatedCluster.ceilingPositions(new OPhysicalPosition(firstPosition));
    while (positions.length > 0) {
      for (OPhysicalPosition position : positions) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.deleteRecord(atomicOperation, position.clusterPosition));
      }

      positions = paginatedCluster.higherPositions(positions[positions.length - 1]);
    }
  }

  protected void checkRecordsSize(final int expectedRecordSize) throws IOException {
    Assert.assertEquals(expectedRecordSize, paginatedCluster.getRecordsSize());
  }

  @Test
  public void testDeleteRecordAndAddNewOnItsPlace() throws IOException {
    byte[] smallRecord = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final int recordVersion = 2;

    OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    final OPhysicalPosition[] physicalPosition = new OPhysicalPosition[1];
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation);
            paginatedCluster.deleteRecord(atomicOperation, physicalPosition[0].clusterPosition);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }

    Assert.assertEquals(0, paginatedCluster.getEntries());
    Assert.assertNull(paginatedCluster.readRecord(physicalPosition[0].clusterPosition, false));
    checkRecordsSize(0);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation -> {
          physicalPosition[0] =
              paginatedCluster.createRecord(
                  smallRecord, recordVersion, (byte) 1, null, atomicOperation);
          paginatedCluster.deleteRecord(atomicOperation, physicalPosition[0].clusterPosition);
        });

    checkRecordsSize(0);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation));

    Assert.assertEquals(physicalPosition[0].recordVersion, recordVersion);
    checkRecordsSize(smallRecord.length);
  }

  @Test
  public void testAddOneSmallRecord() throws IOException {
    byte[] smallRecord = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final int recordVersion = 2;

    final OPhysicalPosition[] physicalPosition = new OPhysicalPosition[1];
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }

    Assert.assertEquals(0, paginatedCluster.getEntries());
    Assert.assertNull(paginatedCluster.readRecord(physicalPosition[0].clusterPosition, false));
    checkRecordsSize(0);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition[0].clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
    checkRecordsSize(smallRecord.length);
  }

  @Test
  public void testAddOneBigRecord() throws IOException {
    byte[] bigRecord = new byte[2 * 65536 + 100];
    Random mersenneTwisterFast = new Random();
    mersenneTwisterFast.nextBytes(bigRecord);

    final int recordVersion = 2;

    final OPhysicalPosition[] physicalPosition = new OPhysicalPosition[1];
    OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 1, null, atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }

    Assert.assertEquals(0, paginatedCluster.getEntries());
    Assert.assertNull(paginatedCluster.readRecord(physicalPosition[0].clusterPosition, false));
    checkRecordsSize(0);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            physicalPosition[0] =
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 1, null, atomicOperation));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition[0].clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
    checkRecordsSize(bigRecord.length);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.deleteRecord(atomicOperation, physicalPosition[0].clusterPosition));
    checkRecordsSize(0);
  }

  @Test
  public void testAddManySmallRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testAddManySmallRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 2, null, atomicOperation);

            positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    final Set<Long> rolledBackRecordSet = new HashSet<>();
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int i = records / 2; i < records; i++) {
              int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
              byte[] smallRecord = new byte[recordSize];
              mersenneTwisterFast.nextBytes(smallRecord);

              final OPhysicalPosition physicalPosition =
                  paginatedCluster.createRecord(
                      smallRecord, recordVersion, (byte) 2, null, atomicOperation);
              rolledBackRecordSet.add(physicalPosition.clusterPosition);
            }
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(totalRecordSize);

    for (long clusterPosition : rolledBackRecordSet) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(clusterPosition, false);
      Assert.assertNull(rawBuffer);
    }

    for (int i = records / 2; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 2, null, atomicOperation);
            positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);

      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testAddManyBigRecords() throws IOException {
    final int records = 5000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testAddManyBigRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;

    int totalRecordSize = 0;
    for (int i = 0; i < records / 2; i++) {
      int recordSize =
          mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
              + OClusterPage.MAX_RECORD_SIZE
              + 1;
      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 2, null, atomicOperation);

            positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Set<Long> rolledBackRecordSet = new HashSet<>();
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int i = records / 2; i < records; i++) {
              int recordSize =
                  mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
                      + OClusterPage.MAX_RECORD_SIZE
                      + 1;
              byte[] bigRecord = new byte[recordSize];
              mersenneTwisterFast.nextBytes(bigRecord);

              final OPhysicalPosition physicalPosition =
                  paginatedCluster.createRecord(
                      bigRecord, recordVersion, (byte) 2, null, atomicOperation);
              rolledBackRecordSet.add(physicalPosition.clusterPosition);
            }
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(totalRecordSize);

    for (long clusterPosition : rolledBackRecordSet) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(clusterPosition, false);
      Assert.assertNull(rawBuffer);
    }

    for (int i = records / 2; i < records; i++) {
      int recordSize =
          mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
              + OClusterPage.MAX_RECORD_SIZE
              + 1;
      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 2, null, atomicOperation);
            positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testAddManyRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testAddManyRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 2, null, atomicOperation);

            positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Set<Long> rolledBackRecordSet = new HashSet<>();
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int i = records / 2; i < records; i++) {
              int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
              byte[] smallRecord = new byte[recordSize];
              mersenneTwisterFast.nextBytes(smallRecord);

              final OPhysicalPosition physicalPosition =
                  paginatedCluster.createRecord(
                      smallRecord, recordVersion, (byte) 2, null, atomicOperation);

              rolledBackRecordSet.add(physicalPosition.clusterPosition);
            }
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(totalRecordSize);

    for (long clusterPosition : rolledBackRecordSet) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(clusterPosition, false);
      Assert.assertNull(rawBuffer);
    }

    for (int i = records / 2; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 2, null, atomicOperation);

            positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testAllocatePositionMap() throws IOException {
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            paginatedCluster.allocatePosition((byte) 'd', atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }

    OPhysicalPosition position =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation -> paginatedCluster.allocatePosition((byte) 'd', atomicOperation));

    Assert.assertTrue(position.clusterPosition >= 0);
    ORawBuffer rec = paginatedCluster.readRecord(position.clusterPosition, false);
    Assert.assertNull(rec);
    checkRecordsSize(0);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.createRecord(new byte[20], 1, (byte) 'd', position, atomicOperation));

    rec = paginatedCluster.readRecord(position.clusterPosition, false);
    Assert.assertNotNull(rec);
    checkRecordsSize(20);
  }

  @Test
  public void testManyAllocatePositionMap() throws IOException {
    final int records = 10000;

    List<OPhysicalPosition> positions = new ArrayList<>();
    for (int i = 0; i < records / 2; i++) {
      OPhysicalPosition position =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation -> paginatedCluster.allocatePosition((byte) 'd', atomicOperation));
      Assert.assertTrue(position.clusterPosition >= 0);
      ORawBuffer rec = paginatedCluster.readRecord(position.clusterPosition, false);
      Assert.assertNull(rec);
      positions.add(position);
    }
    checkRecordsSize(0);

    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            for (int i = records / 2; i < records; i++) {
              OPhysicalPosition position =
                  paginatedCluster.allocatePosition((byte) 'd', atomicOperation);
              Assert.assertTrue(position.clusterPosition >= 0);
              ORawBuffer rec = paginatedCluster.readRecord(position.clusterPosition, false);
              Assert.assertNull(rec);
            }
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(0);

    for (int i = records / 2; i < records; i++) {
      OPhysicalPosition position =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation -> paginatedCluster.allocatePosition((byte) 'd', atomicOperation));
      Assert.assertTrue(position.clusterPosition >= 0);
      ORawBuffer rec = paginatedCluster.readRecord(position.clusterPosition, false);
      Assert.assertNull(rec);
      positions.add(position);
    }
    checkRecordsSize(0);

    for (int i = 0; i < records; i++) {
      OPhysicalPosition position = positions.get(i);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              paginatedCluster.createRecord(
                  new byte[20], 1, (byte) 'd', position, atomicOperation));
      ORawBuffer rec = paginatedCluster.readRecord(position.clusterPosition, false);
      Assert.assertNotNull(rec);
    }
    checkRecordsSize(20 * records);
  }

  @Test
  public void testRemoveHalfSmallRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testRemoveHalfSmallRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      smallRecord, recordVersion, (byte) 2, null, atomicOperation));

      positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int deletedRecords = 0;
              Assert.assertEquals(records, paginatedCluster.getEntries());
              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  Assert.assertTrue(
                      paginatedCluster.deleteRecord(atomicOperation, clusterPosition));
                  deletedRecords++;

                  Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);

      for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
        ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
        Assert.assertNotNull(rawBuffer);

        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> deletedPositions = new HashSet<>();
    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition, false));
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              Assert.assertFalse(paginatedCluster.deleteRecord(atomicOperation, deletedPosition)));
    }
    checkRecordsSize(totalRecordSize);

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testRemoveHalfBigRecords() throws IOException {
    final int records = 5000;
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testRemoveHalfBigRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize =
          mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
              + OClusterPage.MAX_RECORD_SIZE
              + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      bigRecord, recordVersion, (byte) 2, null, atomicOperation));

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      Assert.assertEquals(records, paginatedCluster.getEntries());

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int deletedRecords = 0;
              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  Assert.assertTrue(
                      paginatedCluster.deleteRecord(atomicOperation, clusterPosition));
                  deletedRecords++;

                  Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());
                }
              }

              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);

      for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
        ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
        Assert.assertNotNull(rawBuffer);

        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> deletedPositions = new HashSet<>();
    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition, false));
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              Assert.assertFalse(paginatedCluster.deleteRecord(atomicOperation, deletedPosition)));
    }
    checkRecordsSize(totalRecordSize);

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testRemoveHalfRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testRemoveHalfRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      bigRecord, recordVersion, (byte) 2, null, atomicOperation));

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int deletedRecords = 0;
              Assert.assertEquals(records, paginatedCluster.getEntries());
              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  Assert.assertTrue(
                      paginatedCluster.deleteRecord(atomicOperation, clusterPosition));
                  deletedRecords++;

                  Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);

      for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
        ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
        Assert.assertNotNull(rawBuffer);

        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> deletedPositions = new HashSet<>();
    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition, false));
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              Assert.assertFalse(paginatedCluster.deleteRecord(atomicOperation, deletedPosition)));
    }
    checkRecordsSize(totalRecordSize);

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testRemoveHalfRecordsAndAddAnotherHalfAgain() throws IOException {
    final int records = 10_000;
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);

    System.out.println("testRemoveHalfRecordsAndAddAnotherHalfAgain seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      bigRecord, recordVersion, (byte) 2, null, atomicOperation));

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());

    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        deletedRecords++;

        Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);

        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      bigRecord, recordVersion, (byte) 2, null, atomicOperation));

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Assert.assertEquals(paginatedCluster.getEntries(), (long) (1.5 * records - deletedRecords));

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  @Test
  public void testUpdateOneSmallRecord() throws IOException {
    final byte[] smallRecord = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final int recordVersion = 2;

    OPhysicalPosition physicalPosition =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation));

    checkRecordsSize(smallRecord.length);

    final int updatedRecordVersion = 3;
    final byte[] updatedRecord = new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3};

    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedRecord,
                updatedRecordVersion,
                (byte) 2,
                atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(smallRecord.length);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(recordVersion, rawBuffer.version);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
    Assert.assertEquals(rawBuffer.recordType, 1);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedRecord,
                updatedRecordVersion,
                (byte) 2,
                atomicOperation));
    checkRecordsSize(updatedRecord.length);

    rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

    Assert.assertEquals(rawBuffer.version, updatedRecordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(updatedRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  @Test
  public void testUpdateOneSmallRecordVersionIsLowerCurrentOne() throws IOException {
    final byte[] smallRecord = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final int recordVersion = 2;

    OPhysicalPosition physicalPosition =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation));
    checkRecordsSize(smallRecord.length);

    final int updateRecordVersion = 1;

    final byte[] updatedRecord = new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3};

    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                smallRecord,
                updateRecordVersion,
                (byte) 2,
                atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(smallRecord.length);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);
    Assert.assertNotNull(rawBuffer);
    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedRecord,
                updateRecordVersion,
                (byte) 2,
                atomicOperation));
    rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

    Assert.assertEquals(rawBuffer.version, updateRecordVersion);

    Assertions.assertThat(rawBuffer.buffer).isEqualTo(updatedRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
    checkRecordsSize(updatedRecord.length);
  }

  @Test
  public void testUpdateOneSmallRecordVersionIsMinusTwo() throws IOException {
    final byte[] smallRecord = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final int recordVersion = 2;

    OPhysicalPosition physicalPosition =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 1, null, atomicOperation));
    checkRecordsSize(smallRecord.length);

    final int updateRecordVersion;
    updateRecordVersion = -2;

    final byte[] updatedRecord = new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3};

    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedRecord,
                updateRecordVersion,
                (byte) 2,
                atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(smallRecord.length);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                smallRecord,
                updateRecordVersion,
                (byte) 2,
                atomicOperation));

    rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

    Assert.assertEquals(rawBuffer.version, updateRecordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
    checkRecordsSize(smallRecord.length);
  }

  @Test
  public void testUpdateOneBigRecord() throws IOException {
    final byte[] bigRecord = new byte[2 * 65536 + 100];
    final long seed = System.nanoTime();
    System.out.println("testUpdateOneBigRecord seed " + seed);
    Random mersenneTwisterFast = new Random(seed);

    mersenneTwisterFast.nextBytes(bigRecord);

    final int recordVersion = 2;

    OPhysicalPosition physicalPosition =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 1, null, atomicOperation));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
    checkRecordsSize(bigRecord.length);

    final int updatedRecordVersion = 3;
    final byte[] updatedBigRecord = new byte[2 * 65536 + 20];
    mersenneTwisterFast.nextBytes(updatedBigRecord);

    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedBigRecord,
                updatedRecordVersion,
                (byte) 2,
                atomicOperation);
            throw new RollbackException();
          });
    } catch (RollbackException ignore) {
    }
    checkRecordsSize(bigRecord.length);

    rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
    checkRecordsSize(bigRecord.length);

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            paginatedCluster.updateRecord(
                physicalPosition.clusterPosition,
                updatedBigRecord,
                recordVersion,
                (byte) 2,
                atomicOperation));
    rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(updatedBigRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
    checkRecordsSize(updatedBigRecord.length);
  }

  @Test
  public void testUpdateManySmallRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testUpdateManySmallRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();
    Set<Long> updatedPositions = new HashSet<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    smallRecord, recordVersion, (byte) 2, null, atomicOperation);
            positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    final int newRecordVersion;
    newRecordVersion = recordVersion + 1;

    {
      for (long clusterPosition : positionRecordMap.keySet()) {
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                if (mersenneTwisterFast.nextBoolean()) {
                  int recordSize =
                      mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
                  byte[] smallRecord = new byte[recordSize];
                  mersenneTwisterFast.nextBytes(smallRecord);

                  if (clusterPosition == 100) {
                    System.out.println();
                  }

                  paginatedCluster.updateRecord(
                      clusterPosition, smallRecord, newRecordVersion, (byte) 3, atomicOperation);
                }
                throw new RollbackException();
              });
        } catch (RollbackException ignore) {
        }
      }
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
        byte[] smallRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(smallRecord);

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.updateRecord(
                    clusterPosition, smallRecord, newRecordVersion, (byte) 3, atomicOperation));

        positionRecordMap.put(clusterPosition, smallRecord);
        updatedPositions.add(clusterPosition);
        totalRecordSize += recordSize - record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());

      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);
        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  @Test
  public void testUpdateManyBigRecords() throws IOException {
    final int records = 5000;

    long seed = 1605083213475L; // System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testUpdateManyBigRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();
    Set<Long> updatedPositions = new HashSet<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize =
          mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
              + OClusterPage.MAX_RECORD_SIZE
              + 1;
      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    bigRecord, recordVersion, (byte) 2, null, atomicOperation);
            positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    final int newRecordVersion = recordVersion + 1;
    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  int recordSize =
                      mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
                          + OClusterPage.MAX_RECORD_SIZE
                          + 1;
                  byte[] bigRecord = new byte[recordSize];
                  mersenneTwisterFast.nextBytes(bigRecord);

                  paginatedCluster.updateRecord(
                      clusterPosition, bigRecord, newRecordVersion, (byte) 3, atomicOperation);
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize =
            mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE)
                + OClusterPage.MAX_RECORD_SIZE
                + 1;
        byte[] bigRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(bigRecord);

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.updateRecord(
                    clusterPosition, bigRecord, newRecordVersion, (byte) 3, atomicOperation));

        positionRecordMap.put(clusterPosition, bigRecord);
        updatedPositions.add(clusterPosition);
        totalRecordSize += recordSize - record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());

      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);

        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  @Test
  public void testUpdateManyRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testUpdateManyRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<>();
    Set<Long> updatedPositions = new HashSet<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion, (byte) 2, null, atomicOperation));
      positionRecordMap.put(physicalPosition.clusterPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    final int newRecordVersion = recordVersion + 1;

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  int recordSize =
                      mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
                  byte[] record = new byte[recordSize];
                  mersenneTwisterFast.nextBytes(record);

                  paginatedCluster.updateRecord(
                      clusterPosition, record, newRecordVersion, (byte) 3, atomicOperation);
                }
              }

              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      long clusterPosition = entry.getKey();
      byte[] oldRecord = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
        byte[] record = new byte[recordSize];
        mersenneTwisterFast.nextBytes(record);

        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.updateRecord(
                    clusterPosition, record, newRecordVersion, (byte) 3, atomicOperation));

        positionRecordMap.put(clusterPosition, record);
        updatedPositions.add(clusterPosition);
        totalRecordSize += recordSize - oldRecord.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey(), false);
      Assert.assertNotNull(rawBuffer);

      Assertions.assertThat(rawBuffer.buffer).isEqualTo(entry.getValue());
      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);
        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  @Test
  public void testUpdateRecordNearMaxPageSize() throws IOException {
    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testUpdateRecordNearMaxPageSize seed : " + seed);
    final int recordVersion = 1;
    final int recordSize = OClusterPage.MAX_RECORD_SIZE - 30;
    final byte[] record = new byte[recordSize];
    mersenneTwisterFast.nextBytes(record);

    final OPhysicalPosition physicalPosition =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.createRecord(
                    record, recordVersion, (byte) 2, null, atomicOperation));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

    Assert.assertEquals(recordVersion, rawBuffer.version);
    Assertions.assertThat(rawBuffer.buffer).isEqualTo(record);
    Assert.assertEquals(2L, rawBuffer.recordType);
    checkRecordsSize(recordSize);

    for (int i = 0; i < 40; i++) {
      int updatedRecordVersion = recordVersion + 1 + i;
      final byte[] updatedRecord = new byte[recordSize + i];
      mersenneTwisterFast.nextBytes(record);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              paginatedCluster.updateRecord(
                  physicalPosition.clusterPosition,
                  updatedRecord,
                  updatedRecordVersion,
                  (byte) 2,
                  atomicOperation));

      rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition, false);

      Assert.assertEquals(updatedRecordVersion, rawBuffer.version);
      Assertions.assertThat(rawBuffer.buffer).isEqualTo(updatedRecord);
      Assert.assertEquals(2L, rawBuffer.recordType);
      checkRecordsSize(recordSize + i);
    }
  }

  @Test
  public void testForwardIteration() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testForwardIteration seed : " + seed);

    NavigableMap<Long, byte[]> positionRecordMap = new TreeMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> {
            final OPhysicalPosition physicalPosition =
                paginatedCluster.createRecord(
                    record, recordVersion, (byte) 2, null, atomicOperation);
            positionRecordMap.put(physicalPosition.clusterPosition, record);
          });
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              for (int i = 0; i < records / 2; i++) {
                int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
                byte[] record = new byte[recordSize];
                mersenneTwisterFast.nextBytes(record);

                paginatedCluster.createRecord(
                    record, recordVersion, (byte) 2, null, atomicOperation);
              }

              for (long clusterPosition : positionRecordMap.keySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  Assert.assertTrue(
                      paginatedCluster.deleteRecord(atomicOperation, clusterPosition));
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);
    }

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion, (byte) 2, null, atomicOperation));
      positionRecordMap.put(physicalPosition.clusterPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.clusterPosition = 0;

    OPhysicalPosition[] positions = paginatedCluster.ceilingPositions(physicalPosition);
    Assert.assertTrue(positions.length > 0);

    int counter = 0;
    for (long testedPosition : positionRecordMap.keySet()) {
      Assert.assertTrue(positions.length > 0);
      Assert.assertEquals(positions[0].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[0];
      positions = paginatedCluster.higherPositions(positionToFind);

      counter++;
    }

    Assert.assertEquals(paginatedCluster.getEntries(), counter);

    Assert.assertEquals(paginatedCluster.getFirstPosition(), (long) positionRecordMap.firstKey());
    Assert.assertEquals(paginatedCluster.getLastPosition(), (long) positionRecordMap.lastKey());
  }

  @Test
  public void testBackwardIteration() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testBackwardIteration seed : " + seed);

    NavigableMap<Long, byte[]> positionRecordMap = new TreeMap<>();

    final int recordVersion = 2;
    int totalRecordSize = 0;

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion, (byte) 2, null, atomicOperation));
      positionRecordMap.put(physicalPosition.clusterPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              for (int i = 0; i < records / 2; i++) {
                int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
                byte[] record = new byte[recordSize];
                mersenneTwisterFast.nextBytes(record);

                paginatedCluster.createRecord(
                    record, recordVersion, (byte) 2, null, atomicOperation);
              }

              for (Map.Entry<Long, byte[]> record : positionRecordMap.entrySet()) {
                if (mersenneTwisterFast.nextBoolean()) {
                  Assert.assertTrue(
                      paginatedCluster.deleteRecord(atomicOperation, record.getKey()));
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);
    }

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion, (byte) 2, null, atomicOperation));
      positionRecordMap.put(physicalPosition.clusterPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Iterator<Map.Entry<Long, byte[]>> positionIterator = positionRecordMap.entrySet().iterator();
    while (positionIterator.hasNext()) {
      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long clusterPosition = entry.getKey();
      byte[] record = entry.getValue();
      if (mersenneTwisterFast.nextBoolean()) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertTrue(paginatedCluster.deleteRecord(atomicOperation, clusterPosition)));
        positionIterator.remove();
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.clusterPosition = Long.MAX_VALUE;

    OPhysicalPosition[] positions = paginatedCluster.floorPositions(physicalPosition);
    Assert.assertTrue(positions.length > 0);

    positionIterator = positionRecordMap.descendingMap().entrySet().iterator();
    int counter = 0;
    while (positionIterator.hasNext()) {
      Assert.assertTrue(positions.length > 0);

      Map.Entry<Long, byte[]> entry = positionIterator.next();
      long testedPosition = entry.getKey();
      Assert.assertEquals(positions[positions.length - 1].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[positions.length - 1];
      positions = paginatedCluster.lowerPositions(positionToFind);

      counter++;
    }

    Assert.assertEquals(paginatedCluster.getEntries(), counter);

    Assert.assertEquals(paginatedCluster.getFirstPosition(), (long) positionRecordMap.firstKey());
    Assert.assertEquals(paginatedCluster.getLastPosition(), (long) positionRecordMap.lastKey());
  }

  @Test
  public void testGetPhysicalPosition() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    Random mersenneTwisterFast = new Random(seed);
    System.out.println("testGetPhysicalPosition seed : " + seed);

    Map<OPhysicalPosition, byte[]> positions = new HashMap<>();

    final OModifiableInteger recordVersion = new OModifiableInteger();
    int totalRecordSize = 0;

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      recordVersion.increment();

      final byte recordType = (byte) i;
      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion.value, recordType, null, atomicOperation));
      positions.put(physicalPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    {
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              for (int i = 0; i < records / 2; i++) {
                int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
                byte[] record = new byte[recordSize];
                mersenneTwisterFast.nextBytes(record);

                recordVersion.increment();

                paginatedCluster.createRecord(
                    record, recordVersion.value, (byte) i, null, atomicOperation);
              }

              for (OPhysicalPosition position : positions.keySet()) {
                OPhysicalPosition physicalPosition = new OPhysicalPosition();
                physicalPosition.clusterPosition = position.clusterPosition;

                physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

                Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
                Assert.assertEquals(physicalPosition.recordType, position.recordType);

                Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
                if (mersenneTwisterFast.nextBoolean()) {
                  paginatedCluster.deleteRecord(atomicOperation, position.clusterPosition);
                }
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
      checkRecordsSize(totalRecordSize);
    }

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);
      recordVersion.increment();

      final byte currentType = (byte) i;
      final OPhysicalPosition physicalPosition =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  paginatedCluster.createRecord(
                      record, recordVersion.value, currentType, null, atomicOperation));
      positions.put(physicalPosition, record);
      totalRecordSize += recordSize;
      checkRecordsSize(totalRecordSize);
    }

    Set<OPhysicalPosition> removedPositions = new HashSet<>();
    for (Map.Entry<OPhysicalPosition, byte[]> entry : positions.entrySet()) {
      OPhysicalPosition position = entry.getKey();
      byte[] record = entry.getValue();
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = position.clusterPosition;

      physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

      Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
      Assert.assertEquals(physicalPosition.recordType, position.recordType);

      Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
      if (mersenneTwisterFast.nextBoolean()) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                paginatedCluster.deleteRecord(atomicOperation, position.clusterPosition));
        removedPositions.add(position);
        totalRecordSize -= record.length;
        checkRecordsSize(totalRecordSize);
      }
    }

    for (OPhysicalPosition position : positions.keySet()) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = position.clusterPosition;

      physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

      if (removedPositions.contains(position)) Assert.assertNull(physicalPosition);
      else {
        Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
        Assert.assertEquals(physicalPosition.recordType, position.recordType);

        Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
      }
    }
  }
}

final class RollbackException extends OException implements OHighLevelException {
  public RollbackException() {
    super("");
  }

  public RollbackException(String message) {
    super(message);
  }

  public RollbackException(RollbackException exception) {
    super(exception);
  }
}
