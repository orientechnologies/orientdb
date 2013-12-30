package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.serialization.compression.impl.ONothingCompression;
import com.orientechnologies.orient.core.serialization.compression.impl.OSnappyCompression;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 26.03.13
 */
@Test
public class LocalPaginatedClusterTest {
  private static final int           RECORD_SYSTEM_INFORMATION = 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
                                                                   + OLongSerializer.LONG_SIZE;
  public OPaginatedCluster           paginatedCluster          = new OPaginatedCluster();
  protected String                   buildDirectory;
  protected ODiskCache               diskCache;
  protected OAtomicOperationsManager atomicOperationsManager;

  @BeforeClass
  public void beforeClass() throws IOException {
    System.out.println("Start LocalPaginatedClusterTest");
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += "/localPaginatedClusterTest";

    OLocalPaginatedStorage storage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    diskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 2648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, storage, null, false, false);
    atomicOperationsManager = new OAtomicOperationsManager(null);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);

    when(storage.getDiskCache()).thenReturn(diskCache);
    when(storage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);
    when(storage.getVariableParser()).thenReturn(variableParser);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");
    when(storage.getStoragePath()).thenReturn(buildDirectory);

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    paginatedCluster.configure(storage, 5, "paginatedClusterTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  @AfterClass
  public void afterClass() throws IOException {
    paginatedCluster.delete();

    diskCache.delete();

    File file = new File(buildDirectory);
    Assert.assertTrue(file.delete());

    System.out.println("End LocalPaginatedClusterTest");
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    paginatedCluster.truncate();
  }

  public void testDeleteRecordAndAddNewOnItsPlace() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));
    paginatedCluster.deleteRecord(physicalPosition.clusterPosition);

    physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(1));

    recordVersion.increment();
    Assert.assertEquals(physicalPosition.recordVersion, recordVersion);
  }

  public void testAddOneSmallRecord() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
  }

  public void testAddOneBigRecord() throws IOException {
    byte[] bigRecord = new byte[2 * 65536 + 100];
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast();
    mersenneTwisterFast.nextBytes(bigRecord);

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
  }

  public void testAddManySmallRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testAddManySmallRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testAddManyBigRecords() throws IOException {
    final int records = 5000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testAddManyBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + OClusterPage.MAX_RECORD_SIZE + 1;
      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testAddManyRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testAddManyRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testRemoveHalfSmallRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testRemoveHalfSmallRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<OClusterPosition> deletedPositions = new HashSet<OClusterPosition>();
    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (OClusterPosition deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testRemoveHalfBigRecords() throws IOException {
    final int records = 5000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testRemoveHalfBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + OClusterPage.MAX_RECORD_SIZE + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<OClusterPosition> deletedPositions = new HashSet<OClusterPosition>();
    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (OClusterPosition deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testRemoveHalfRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testRemoveHalfRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<OClusterPosition> deletedPositions = new HashSet<OClusterPosition>();
    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (OClusterPosition deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testRemoveHalfRecordsAndAddAnotherHalfAgain() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testRemoveHalfRecordsAndAddAnotherHalfAgain seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    int deletedRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());

    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    Assert.assertEquals(paginatedCluster.getEntries(), (long) (1.5 * records - deletedRecords));
  }

  public void testUpdateOneSmallRecord() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    recordVersion.increment();
    smallRecord = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 };
    paginatedCluster.updateRecord(physicalPosition.clusterPosition, smallRecord, recordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  public void testUpdateOneSmallRecordVersionIsLowerCurrentOne() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    ORecordVersion updateRecordVersion = OVersionFactory.instance().createVersion();
    updateRecordVersion.increment();

    smallRecord = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 };
    paginatedCluster.updateRecord(physicalPosition.clusterPosition, smallRecord, updateRecordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  public void testUpdateOneSmallRecordVersionIsMinusTwo() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    ORecordVersion updateRecordVersion = OVersionFactory.instance().createVersion();
    updateRecordVersion.setCounter(-2);

    smallRecord = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 };
    paginatedCluster.updateRecord(physicalPosition.clusterPosition, smallRecord, updateRecordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  public void testUpdateOneBigRecord() throws IOException {
    byte[] bigRecord = new byte[2 * 65536 + 100];
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast();
    mersenneTwisterFast.nextBytes(bigRecord);

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, OClusterPositionFactory.INSTANCE.valueOf(0));

    recordVersion.increment();
    bigRecord = new byte[2 * 65536 + 20];
    mersenneTwisterFast.nextBytes(bigRecord);

    paginatedCluster.updateRecord(physicalPosition.clusterPosition, bigRecord, recordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  public void testUpdateManySmallRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testUpdateManySmallRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();
    Set<OClusterPosition> updatedPositions = new HashSet<OClusterPosition>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
      byte[] smallRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(smallRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, smallRecord);
    }

    ORecordVersion newRecordVersion = OVersionFactory.instance().createVersion();
    newRecordVersion.copyFrom(recordVersion);
    newRecordVersion.increment();

    for (OClusterPosition clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
        byte[] smallRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(smallRecord);

        paginatedCluster.updateRecord(clusterPosition, smallRecord, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, smallRecord);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.buffer, entry.getValue());

      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);
        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  public void testUpdateManyBigRecords() throws IOException {
    final int records = 5000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testUpdateManyBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();
    Set<OClusterPosition> updatedPositions = new HashSet<OClusterPosition>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + OClusterPage.MAX_RECORD_SIZE + 1;
      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);
      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    ORecordVersion newRecordVersion = OVersionFactory.instance().createVersion();
    newRecordVersion.copyFrom(recordVersion);
    newRecordVersion.increment();

    for (OClusterPosition clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + OClusterPage.MAX_RECORD_SIZE + 1;
        byte[] bigRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(bigRecord);

        paginatedCluster.updateRecord(clusterPosition, bigRecord, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, bigRecord);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.buffer, entry.getValue());

      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);
        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  public void testUpdateManyRecords() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testUpdateManyRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();
    Set<OClusterPosition> updatedPositions = new HashSet<OClusterPosition>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, recordVersion, (byte) 2);
      positionRecordMap.put(physicalPosition.clusterPosition, record);
    }

    ORecordVersion newRecordVersion = OVersionFactory.instance().createVersion();
    newRecordVersion.copyFrom(recordVersion);
    newRecordVersion.increment();

    for (OClusterPosition clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
        byte[] record = new byte[recordSize];
        mersenneTwisterFast.nextBytes(record);

        paginatedCluster.updateRecord(clusterPosition, record, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, record);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<OClusterPosition, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.buffer, entry.getValue());

      if (updatedPositions.contains(entry.getKey())) {
        Assert.assertEquals(rawBuffer.version, newRecordVersion);
        Assert.assertEquals(rawBuffer.recordType, 3);
      } else {
        Assert.assertEquals(rawBuffer.version, recordVersion);
        Assert.assertEquals(rawBuffer.recordType, 2);
      }
    }
  }

  public void testForwardIteration() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testForwardIteration seed : " + seed);

    NavigableMap<OClusterPosition, byte[]> positionRecordMap = new TreeMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, recordVersion, (byte) 2);
      positionRecordMap.put(physicalPosition.clusterPosition, record);
    }

    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        positionIterator.remove();
      }
    }

    OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(0);

    OPhysicalPosition[] positions = paginatedCluster.ceilingPositions(physicalPosition);
    Assert.assertTrue(positions.length > 0);

    int counter = 0;
    for (OClusterPosition testedPosition : positionRecordMap.keySet()) {
      Assert.assertTrue(positions.length > 0);
      Assert.assertEquals(positions[0].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[0];
      positions = paginatedCluster.higherPositions(positionToFind);

      counter++;
    }

    Assert.assertEquals(paginatedCluster.getEntries(), counter);

    Assert.assertEquals(paginatedCluster.getFirstPosition(), positionRecordMap.firstKey());
    Assert.assertEquals(paginatedCluster.getLastPosition(), positionRecordMap.lastKey());
  }

  public void testBackwardIteration() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(1381162033616L);
    System.out.println("testBackwardIteration seed : " + seed);

    NavigableMap<OClusterPosition, byte[]> positionRecordMap = new TreeMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, recordVersion, (byte) 2);
      positionRecordMap.put(physicalPosition.clusterPosition, record);
    }

    Iterator<OClusterPosition> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      OClusterPosition clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        positionIterator.remove();
      }
    }

    OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(Long.MAX_VALUE);

    OPhysicalPosition[] positions = paginatedCluster.floorPositions(physicalPosition);
    Assert.assertTrue(positions.length > 0);

    positionIterator = positionRecordMap.descendingKeySet().iterator();
    int counter = 0;
    while (positionIterator.hasNext()) {
      Assert.assertTrue(positions.length > 0);

      OClusterPosition testedPosition = positionIterator.next();
      Assert.assertEquals(positions[positions.length - 1].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[positions.length - 1];
      positions = paginatedCluster.lowerPositions(positionToFind);

      counter++;
    }

    Assert.assertEquals(paginatedCluster.getEntries(), counter);

    Assert.assertEquals(paginatedCluster.getFirstPosition(), positionRecordMap.firstKey());
    Assert.assertEquals(paginatedCluster.getLastPosition(), positionRecordMap.lastKey());
  }

  public void testGetPhysicalPosition() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testGetPhysicalPosition seed : " + seed);

    Set<OPhysicalPosition> positions = new HashSet<OPhysicalPosition>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);
      recordVersion.increment();

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, recordVersion, (byte) i);
      positions.add(physicalPosition);
    }

    Set<OPhysicalPosition> removedPositions = new HashSet<OPhysicalPosition>();
    for (OPhysicalPosition position : positions) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = position.clusterPosition;

      physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

      Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
      Assert.assertEquals(physicalPosition.recordType, position.recordType);

      Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
      Assert.assertEquals(physicalPosition.dataSegmentPos, position.dataSegmentPos);
      Assert.assertEquals(physicalPosition.dataSegmentId, position.dataSegmentId);
      if (mersenneTwisterFast.nextBoolean()) {
        paginatedCluster.deleteRecord(position.clusterPosition);
        removedPositions.add(position);
      }
    }

    for (OPhysicalPosition position : positions) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = position.clusterPosition;

      physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

      if (removedPositions.contains(position))
        Assert.assertNull(physicalPosition);
      else {
        Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
        Assert.assertEquals(physicalPosition.recordType, position.recordType);

        Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
        Assert.assertEquals(physicalPosition.dataSegmentPos, position.dataSegmentPos);
        Assert.assertEquals(physicalPosition.dataSegmentId, position.dataSegmentId);
      }
    }
  }

  public void testCompressionNothing() throws Exception {
    paginatedCluster.set(OCluster.ATTRIBUTES.COMPRESSION, ONothingCompression.NAME);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 1);

    byte[] record = new byte[100];
    Random random = new Random();
    random.nextBytes(record);

    OPhysicalPosition physicalPosition = paginatedCluster
        .createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    OCacheEntry cacheEntry = diskCache.load(1, 1, false);
    OCachePointer pagePointer = cacheEntry.getCachePointer();

    OClusterPage page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);
    int recordIndex = (int) (physicalPosition.clusterPosition.longValue() & 0xFFFF);

    byte[] storedEntity = page.getRecordBinaryValue(recordIndex, 0, page.getRecordSize(recordIndex));
    byte[] storedRecord = new byte[100];
    System.arraycopy(storedEntity, OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, storedRecord, 0, storedRecord.length);

    Assert.assertEquals(storedRecord, record);
    diskCache.release(cacheEntry);
  }

  public void testCompressionSnappy() throws Exception {
    paginatedCluster.set(OCluster.ATTRIBUTES.COMPRESSION, OSnappyCompression.NAME);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 1);

    byte[] record = new byte[100];
    Random random = new Random();
    random.nextBytes(record);

    OPhysicalPosition physicalPosition = paginatedCluster
        .createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    record = OSnappyCompression.INSTANCE.compress(record);

    OCacheEntry cacheEntry = diskCache.load(1, 1, false);
    OCachePointer pagePointer = cacheEntry.getCachePointer();
    int recordIndex = (int) (physicalPosition.clusterPosition.longValue() & 0xFFFF);

    OClusterPage page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);

    byte[] storedEntity = page.getRecordBinaryValue(recordIndex, 0, page.getRecordSize(recordIndex));
    byte[] storedRecord = new byte[record.length];
    System.arraycopy(storedEntity, OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE, storedRecord, 0, storedRecord.length);

    Assert.assertEquals(storedRecord, record);
    diskCache.release(cacheEntry);
  }

  public void testRecordGrowFactor() throws Exception {
    paginatedCluster.set(OCluster.ATTRIBUTES.COMPRESSION, ONothingCompression.NAME);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 1.5);

    byte[] record = new byte[100];
    Random random = new Random();
    random.nextBytes(record);

    OPhysicalPosition physicalPosition = paginatedCluster
        .createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    OCacheEntry cacheEntry = diskCache.load(1, 1, false);
    OCachePointer pagePointer = cacheEntry.getCachePointer();

    OClusterPage page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);
    int recordIndex = (int) (physicalPosition.clusterPosition.longValue() & 0xFFFF);

    Assert.assertEquals(page.getRecordSize(recordIndex), ((int) (record.length * 1.5)) + RECORD_SYSTEM_INFORMATION);
    diskCache.release(cacheEntry);

    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 2);
    physicalPosition = paginatedCluster.createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    recordIndex = (int) (physicalPosition.clusterPosition.longValue() & 0xFFFF);
    cacheEntry = diskCache.load(1, 1, false);
    pagePointer = cacheEntry.getCachePointer();

    page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);

    Assert.assertEquals(page.getRecordSize(recordIndex), record.length * 2 + RECORD_SYSTEM_INFORMATION);
    diskCache.release(cacheEntry);
  }

  public void testRecordOverflowGrowFactor() throws Exception {
    paginatedCluster.set(OCluster.ATTRIBUTES.COMPRESSION, ONothingCompression.NAME);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 1.5);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR, 2.5);

    byte[] record = new byte[100];
    Random random = new Random();
    random.nextBytes(record);

    ORecordVersion version = OVersionFactory.instance().createVersion();
    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, version, (byte) 1);

    record = new byte[150];
    random.nextBytes(record);

    paginatedCluster.updateRecord(physicalPosition.clusterPosition, record, version, (byte) 1);

    OCacheEntry cacheEntry = diskCache.load(1, 1, false);
    int recordIndex = (int) (physicalPosition.clusterPosition.longValue() & 0xFFFF);
    OCachePointer pagePointer = cacheEntry.getCachePointer();

    OClusterPage page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);

    Assert.assertEquals(page.getRecordSize(recordIndex), record.length + RECORD_SYSTEM_INFORMATION);
    diskCache.release(cacheEntry);

    record = new byte[200];
    random.nextBytes(record);

    paginatedCluster.updateRecord(physicalPosition.clusterPosition, record, version, (byte) 1);

    cacheEntry = diskCache.load(1, 1, false);
    pagePointer = cacheEntry.getCachePointer();

    page = new OClusterPage(pagePointer.getDataPointer(), false, ODurablePage.TrackMode.NONE);

    int fullContentSize = 500 + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE; // type + real size

    Assert.assertEquals(page.getRecordSize(recordIndex), 150 + RECORD_SYSTEM_INFORMATION);
    fullContentSize -= 150 + RECORD_SYSTEM_INFORMATION - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE;

    Assert.assertEquals(page.getRecordSize(recordIndex + 1), fullContentSize
        + (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE));
    diskCache.release(cacheEntry);
  }
}
