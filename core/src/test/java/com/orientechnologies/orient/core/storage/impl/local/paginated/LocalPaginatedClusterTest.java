package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.compression.impl.ONothingCompression;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin
 * @since 26.03.13
 */
@Test
public class LocalPaginatedClusterTest {
  private static final int           RECORD_SYSTEM_INFORMATION = 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
                                                                   + OLongSerializer.LONG_SIZE;
  public OPaginatedCluster           paginatedCluster;
  protected String                   buildDirectory;

  protected O2QCache                 readCache;
  protected OWriteCache              writeCache;

  protected OAtomicOperationsManager atomicOperationsManager;
  private OContextConfiguration      contextConfiguration      = new OContextConfiguration();

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
    storageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;
    when(storage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(storageConfiguration));
    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);
    when(storageConfiguration.getContextConfiguration()).thenReturn(contextConfiguration);
    when(storage.getStoragePath()).thenReturn(buildDirectory);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);
    when(storage.getVariableParser()).thenReturn(variableParser);

    writeCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, null, 100,
        2648L * 1024 * 1024, 2648L * 1024 * 1024 + 400L * 1024 * 1024 * 1024, storage, true, 1);

    readCache = new O2QCache(400L * 1024 * 1024 * 1024, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, false);

    atomicOperationsManager = new OAtomicOperationsManager(storage);

    when(storage.getReadCache()).thenReturn(readCache);
    when(storage.getWriteCache()).thenReturn(writeCache);

    when(storage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);

    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    paginatedCluster = new OPaginatedCluster("paginatedClusterTest", storage);
    paginatedCluster.configure(storage, 5, "paginatedClusterTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  @AfterClass
  public void afterClass() throws IOException {
    paginatedCluster.delete();

    readCache.deleteStorage(writeCache);

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
    Assert.assertEquals(physicalPosition.clusterPosition, 0);
    paginatedCluster.deleteRecord(physicalPosition.clusterPosition);

    recordVersion = OVersionFactory.instance().createVersion();
    Assert.assertEquals(recordVersion.getCounter(), 0);
    physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, 1);

    Assert.assertEquals(physicalPosition.recordVersion, recordVersion);
  }

  public void testAddOneSmallRecord() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

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
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, recordVersion);
    Assert.assertEquals(rawBuffer.buffer, bigRecord);
    Assert.assertEquals(rawBuffer.recordType, 1);
  }

  public void testAddManySmallRecords() throws IOException {
    final int records = 10000;

    long seed = 1426587095601L;
    System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);
    System.out.println("testAddManySmallRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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
    Set<Long> deletedPositions = new HashSet<Long>();
    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testHideHalfSmallRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testHideHalfSmallRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    int hiddenRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> hiddenPositions = new HashSet<Long>();
    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        hiddenPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.hideRecord(clusterPosition));
        hiddenRecords++;

        Assert.assertEquals(records - hiddenRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - hiddenRecords);
    for (long deletedPosition : hiddenPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.hideRecord(deletedPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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
    Set<Long> deletedPositions = new HashSet<Long>();
    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testHideHalfBigRecords() throws IOException {
    final int records = 5000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testHideHalfBigRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    int hiddenRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> hiddenPositions = new HashSet<Long>();

    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        hiddenPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.hideRecord(clusterPosition));
        hiddenRecords++;

        Assert.assertEquals(records - hiddenRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - hiddenRecords);
    for (long hiddenPosition : hiddenPositions) {
      Assert.assertNull(paginatedCluster.readRecord(hiddenPosition));
      Assert.assertFalse(paginatedCluster.hideRecord(hiddenPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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
    Set<Long> deletedPositions = new HashSet<Long>();
    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        deletedPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        deletedRecords++;

        Assert.assertEquals(records - deletedRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - deletedRecords);
    for (long deletedPosition : deletedPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.deleteRecord(deletedPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
      ORawBuffer rawBuffer = paginatedCluster.readRecord(entry.getKey());
      Assert.assertNotNull(rawBuffer);

      Assert.assertEquals(rawBuffer.version, recordVersion);
      Assert.assertEquals(rawBuffer.buffer, entry.getValue());
      Assert.assertEquals(rawBuffer.recordType, 2);
    }
  }

  public void testHideHalfRecords() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testHideHalfRecords seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    int hiddenRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());
    Set<Long> hiddenPositions = new HashSet<Long>();
    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        hiddenPositions.add(clusterPosition);
        Assert.assertTrue(paginatedCluster.hideRecord(clusterPosition));
        hiddenRecords++;

        Assert.assertEquals(records - hiddenRecords, paginatedCluster.getEntries());

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - hiddenRecords);
    for (long deletedPosition : hiddenPositions) {
      Assert.assertNull(paginatedCluster.readRecord(deletedPosition));
      Assert.assertFalse(paginatedCluster.hideRecord(deletedPosition));
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
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

  public void testHideHalfRecordsAndAddAnotherHalfAgain() throws IOException {
    final int records = 10000;
    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    System.out.println("testHideHalfRecordsAndAddAnotherHalfAgain seed : " + seed);

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();

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

    int hiddenRecords = 0;
    Assert.assertEquals(records, paginatedCluster.getEntries());

    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.hideRecord(clusterPosition));
        hiddenRecords++;

        Assert.assertEquals(paginatedCluster.getEntries(), records - hiddenRecords);

        positionIterator.remove();
      }
    }

    Assert.assertEquals(paginatedCluster.getEntries(), records - hiddenRecords);

    for (int i = 0; i < records / 2; i++) {
      int recordSize = mersenneTwisterFast.nextInt(3 * OClusterPage.MAX_RECORD_SIZE) + 1;

      byte[] bigRecord = new byte[recordSize];
      mersenneTwisterFast.nextBytes(bigRecord);

      final OPhysicalPosition physicalPosition = paginatedCluster.createRecord(bigRecord, recordVersion, (byte) 2);

      positionRecordMap.put(physicalPosition.clusterPosition, bigRecord);
    }

    Assert.assertEquals(paginatedCluster.getEntries(), (long) (1.5 * records - hiddenRecords));
  }

  public void testUpdateOneSmallRecord() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

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
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

    ORecordVersion updateRecordVersion = OVersionFactory.instance().createVersion();
    updateRecordVersion.increment();

    smallRecord = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 };
    paginatedCluster.updateRecord(physicalPosition.clusterPosition, smallRecord, updateRecordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, updateRecordVersion);
    Assert.assertEquals(rawBuffer.buffer, smallRecord);
    Assert.assertEquals(rawBuffer.recordType, 2);
  }

  public void testUpdateOneSmallRecordVersionIsMinusTwo() throws IOException {
    byte[] smallRecord = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.increment();
    recordVersion.increment();

    OPhysicalPosition physicalPosition = paginatedCluster.createRecord(smallRecord, recordVersion, (byte) 1);
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

    ORecordVersion updateRecordVersion = OVersionFactory.instance().createVersion();
    updateRecordVersion.setCounter(-2);

    smallRecord = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 };
    paginatedCluster.updateRecord(physicalPosition.clusterPosition, smallRecord, updateRecordVersion, (byte) 2);

    ORawBuffer rawBuffer = paginatedCluster.readRecord(physicalPosition.clusterPosition);
    Assert.assertNotNull(rawBuffer);

    Assert.assertEquals(rawBuffer.version, updateRecordVersion);
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
    Assert.assertEquals(physicalPosition.clusterPosition, 0);

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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();
    Set<Long> updatedPositions = new HashSet<Long>();

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

    for (long clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(OClusterPage.MAX_RECORD_SIZE - 1) + 1;
        byte[] smallRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(smallRecord);

        paginatedCluster.updateRecord(clusterPosition, smallRecord, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, smallRecord);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();
    Set<Long> updatedPositions = new HashSet<Long>();

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

    for (long clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + OClusterPage.MAX_RECORD_SIZE + 1;
        byte[] bigRecord = new byte[recordSize];
        mersenneTwisterFast.nextBytes(bigRecord);

        paginatedCluster.updateRecord(clusterPosition, bigRecord, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, bigRecord);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    Map<Long, byte[]> positionRecordMap = new HashMap<Long, byte[]>();
    Set<Long> updatedPositions = new HashSet<Long>();

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

    for (long clusterPosition : positionRecordMap.keySet()) {
      if (mersenneTwisterFast.nextBoolean()) {
        int recordSize = mersenneTwisterFast.nextInt(2 * OClusterPage.MAX_RECORD_SIZE) + 1;
        byte[] record = new byte[recordSize];
        mersenneTwisterFast.nextBytes(record);

        paginatedCluster.updateRecord(clusterPosition, record, newRecordVersion, (byte) 3);

        positionRecordMap.put(clusterPosition, record);
        updatedPositions.add(clusterPosition);
      }
    }

    for (Map.Entry<Long, byte[]> entry : positionRecordMap.entrySet()) {
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

    NavigableMap<Long, byte[]> positionRecordMap = new TreeMap<Long, byte[]>();

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

    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        positionIterator.remove();
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

  public void testBackwardIteration() throws IOException {
    final int records = 10000;

    long seed = System.currentTimeMillis();
    MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(1381162033616L);
    System.out.println("testBackwardIteration seed : " + seed);

    NavigableMap<Long, byte[]> positionRecordMap = new TreeMap<Long, byte[]>();

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

    Iterator<Long> positionIterator = positionRecordMap.keySet().iterator();
    while (positionIterator.hasNext()) {
      long clusterPosition = positionIterator.next();
      if (mersenneTwisterFast.nextBoolean()) {
        Assert.assertTrue(paginatedCluster.deleteRecord(clusterPosition));
        positionIterator.remove();
      }
    }

    OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.clusterPosition = Long.MAX_VALUE;

    OPhysicalPosition[] positions = paginatedCluster.floorPositions(physicalPosition);
    Assert.assertTrue(positions.length > 0);

    positionIterator = positionRecordMap.descendingKeySet().iterator();
    int counter = 0;
    while (positionIterator.hasNext()) {
      Assert.assertTrue(positions.length > 0);

      long testedPosition = positionIterator.next();
      Assert.assertEquals(positions[positions.length - 1].clusterPosition, testedPosition);

      OPhysicalPosition positionToFind = positions[positions.length - 1];
      positions = paginatedCluster.lowerPositions(positionToFind);

      counter++;
    }

    Assert.assertEquals(paginatedCluster.getEntries(), counter);

    Assert.assertEquals(paginatedCluster.getFirstPosition(), (long) positionRecordMap.firstKey());
    Assert.assertEquals(paginatedCluster.getLastPosition(), (long) positionRecordMap.lastKey());
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
      }
    }
  }

  @Test(enabled = false)
  public void testRecordGrowFactor() throws Exception {
    paginatedCluster.set(OCluster.ATTRIBUTES.COMPRESSION, ONothingCompression.NAME);
    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 1.5);

    byte[] record = new byte[100];
    Random random = new Random();
    random.nextBytes(record);

    OPhysicalPosition physicalPosition = paginatedCluster
        .createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    OCacheEntry cacheEntry = readCache.load(1, 1, false, writeCache);
    OClusterPage page = new OClusterPage(cacheEntry, false, null);
    int recordIndex = (int) (physicalPosition.clusterPosition & 0xFFFF);

    Assert.assertEquals(page.getRecordSize(recordIndex), ((int) (record.length * 1.5)) + RECORD_SYSTEM_INFORMATION);
    readCache.release(cacheEntry, writeCache);

    paginatedCluster.set(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR, 2);
    physicalPosition = paginatedCluster.createRecord(record, OVersionFactory.instance().createVersion(), (byte) 1);

    recordIndex = (int) (physicalPosition.clusterPosition & 0xFFFF);
    cacheEntry = readCache.load(1, 1, false, writeCache);
    page = new OClusterPage(cacheEntry, false, null);

    Assert.assertEquals(page.getRecordSize(recordIndex), record.length * 2 + RECORD_SYSTEM_INFORMATION);
    readCache.release(cacheEntry, writeCache);
  }

  @Test(enabled = false)
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

    OCacheEntry cacheEntry = readCache.load(1, 1, false, writeCache);
    int recordIndex = (int) (physicalPosition.clusterPosition & 0xFFFF);
    OClusterPage page = new OClusterPage(cacheEntry, false, null);

    Assert.assertEquals(page.getRecordSize(recordIndex), record.length + RECORD_SYSTEM_INFORMATION);
    readCache.release(cacheEntry, writeCache);

    record = new byte[200];
    random.nextBytes(record);

    paginatedCluster.updateRecord(physicalPosition.clusterPosition, record, version, (byte) 1);

    cacheEntry = readCache.load(1, 1, false, writeCache);
    page = new OClusterPage(cacheEntry, false, null);

    int fullContentSize = 500 + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE; // type + real size

    Assert.assertEquals(page.getRecordSize(recordIndex), 150 + RECORD_SYSTEM_INFORMATION);
    fullContentSize -= 150 + RECORD_SYSTEM_INFORMATION - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE;

    Assert.assertEquals(page.getRecordSize(recordIndex + 1), fullContentSize
        + (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE));
    readCache.release(cacheEntry, writeCache);
  }
}
