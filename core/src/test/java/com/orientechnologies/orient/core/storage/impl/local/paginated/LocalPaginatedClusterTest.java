package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OLRUCache;
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
  public OLocalPaginatedCluster paginatedCluster = new OLocalPaginatedCluster();
  private String                buildDirectory;
  private ODiskCache            diskCache;

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

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    diskCache = new OLRUCache(2L * 1024 * 1024 * 1024, directMemory, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger(),
        storage, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);

    when(storage.getDiskCache()).thenReturn(diskCache);
    when(storage.getVariableParser()).thenReturn(variableParser);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    paginatedCluster.configure(storage, 5, "paginatedClusterTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  @AfterClass
  public void afterClass() throws IOException {
    paginatedCluster.delete();
    File file = new File(buildDirectory);
    file.delete();
    diskCache.clear();

    System.out.println("End LocalPaginatedClusterTest");
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    paginatedCluster.truncate();
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
      int recordSize = mersenneTwisterFast.nextInt(OLocalPage.MAX_RECORD_SIZE - 1) + 1;
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
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testAddManyBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + OLocalPage.MAX_RECORD_SIZE + 1;
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
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
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
      int recordSize = mersenneTwisterFast.nextInt(OLocalPage.MAX_RECORD_SIZE - 1) + 1;
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
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testRemoveHalfBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + OLocalPage.MAX_RECORD_SIZE + 1;

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
      int recordSize = mersenneTwisterFast.nextInt(3 * OLocalPage.MAX_RECORD_SIZE) + 1;

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
      int recordSize = mersenneTwisterFast.nextInt(3 * OLocalPage.MAX_RECORD_SIZE) + 1;

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
      int recordSize = mersenneTwisterFast.nextInt(3 * OLocalPage.MAX_RECORD_SIZE) + 1;

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
      int recordSize = mersenneTwisterFast.nextInt(OLocalPage.MAX_RECORD_SIZE - 1) + 1;
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
        int recordSize = mersenneTwisterFast.nextInt(OLocalPage.MAX_RECORD_SIZE - 1) + 1;
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
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testUpdateManyBigRecords seed : " + seed);

    Map<OClusterPosition, byte[]> positionRecordMap = new HashMap<OClusterPosition, byte[]>();
    Set<OClusterPosition> updatedPositions = new HashSet<OClusterPosition>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + OLocalPage.MAX_RECORD_SIZE + 1;
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
        int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + OLocalPage.MAX_RECORD_SIZE + 1;
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
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
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
        int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
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
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
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
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testBackwardIteration seed : " + seed);

    NavigableMap<OClusterPosition, byte[]> positionRecordMap = new TreeMap<OClusterPosition, byte[]>();

    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    for (int i = 0; i < records; i++) {
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
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
      Assert.assertEquals(positions[0].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[0];
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
      int recordSize = mersenneTwisterFast.nextInt(2 * OLocalPage.MAX_RECORD_SIZE) + 1;
      byte[] record = new byte[recordSize];
      mersenneTwisterFast.nextBytes(record);
      recordVersion.increment();

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(record, recordVersion, (byte) i);
      positions.add(physicalPosition);
    }

    for (OPhysicalPosition position : positions) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = position.clusterPosition;

      physicalPosition = paginatedCluster.getPhysicalPosition(physicalPosition);

      Assert.assertEquals(physicalPosition.clusterPosition, position.clusterPosition);
      Assert.assertEquals(physicalPosition.recordType, position.recordType);

      Assert.assertEquals(physicalPosition.recordSize, position.recordSize);
      Assert.assertEquals(physicalPosition.dataSegmentPos, position.dataSegmentPos);
      Assert.assertEquals(physicalPosition.dataSegmentId, position.dataSegmentId);
    }
  }
}
