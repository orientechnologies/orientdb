package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterLocalLHPEOverflowConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterLocalLHPEStatisticConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterLHPEPSConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public class OClusterLocalLHPEPS extends OSharedResourceAdaptive implements OCluster {
  public static final String                                    TYPE                   = "PHYSICAL";
  private static final String                                   DEF_EXTENSION          = ".ocl";

  private static final int                                      INITIAL_D              = 10;

  private static final int                                      DEF_SIZE               = (1 << (INITIAL_D + 1))
                                                                                           * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;

  private static final int                                      GROUP_COUNT            = 2;

  private static final double                                   storageSplitOverflow   = 0.75;
  private static final double                                   storageMergeOverflow   = 0.5;

  private long                                                  lastOverflowBucket     = -1;

  private long                                                  recordSplitPointer     = 0;
  private long                                                  roundCapacity;

  private int                                                   g;
  private int                                                   d;

  private long                                                  pageSize;
  private long                                                  nextPageSize;

  private long                                                  size;

  private long                                                  mainBucketsSize;

  private BitSet                                                splittedBuckets;

  private Map<Long, Integer>                                    mainBucketOverflowInfoByIndex;
  private Map<Long, Integer>                                    groupBucketOverflowInfoByIndex;

  private Map<Long, Integer>[]                                  groupBucketOverflowInfoByChainLength;
  private int                                                   maxChainIndex;

  private OMultiFileSegment                                     fileSegment;

  private int                                                   id;
  private OStoragePhysicalClusterLHPEPSConfiguration            config;
  private String                                                name;

  private OClusterLocalLHPEOverflow                             overflowSegment;
  private OClusterLocalLHPEStatistic                            overflowStatistic;

  private final ThreadLocal<Map<Long, OClusterLocalLHPEBucket>> mainBucketCache        = new ThreadLocal<Map<Long, OClusterLocalLHPEBucket>>() {
                                                                                         @Override
                                                                                         protected Map<Long, OClusterLocalLHPEBucket> initialValue() {
                                                                                           return new HashMap<Long, OClusterLocalLHPEBucket>(
                                                                                               32);
                                                                                         }
                                                                                       };

  private final ThreadLocal<Map<Long, OClusterLocalLHPEBucket>> overflowBucketCache    = new ThreadLocal<Map<Long, OClusterLocalLHPEBucket>>() {
                                                                                         @Override
                                                                                         protected Map<Long, OClusterLocalLHPEBucket> initialValue() {
                                                                                           return new HashMap<Long, OClusterLocalLHPEBucket>(
                                                                                               32);
                                                                                         }
                                                                                       };

  private final Set<OClusterLocalLHPEBucket>                    mainBucketsToStore     = new HashSet<OClusterLocalLHPEBucket>(32);
  private final Set<OClusterLocalLHPEBucket>                    overflowBucketsToStore = new HashSet<OClusterLocalLHPEBucket>(32);

  private OStorageLocal                                         storage;

  private static final int                                      DEFAULT_BUFFER_SIZE    = 1024;

  private boolean                                               isOpen = false;

  public OClusterLocalLHPEPS() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());

    initState();
  }

  public void configure(OStorage iStorage, int iId, String iClusterName, String iLocation, int iDataSegmentId,
      Object... iParameters) throws IOException {
    acquireExclusiveLock();
    try {
      storage = (OStorageLocal) iStorage;
      config = new OStoragePhysicalClusterLHPEPSConfiguration(iStorage.getConfiguration(), iId, iDataSegmentId);
      config.name = iClusterName;
      init(iStorage, iId, iClusterName, iDataSegmentId);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    acquireExclusiveLock();
    try {
      config = (OStoragePhysicalClusterLHPEPSConfiguration) iConfig;
      storage = (OStorageLocal) iStorage;
      init(iStorage, config.getId(), config.getName(), config.getDataSegmentId());
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(int iStartSize) throws IOException {
    acquireExclusiveLock();
    try {
      if (iStartSize == -1)
        iStartSize = DEF_SIZE;

      if (config.root.clusters.size() <= config.id)
        config.root.clusters.add(config);
      else
        config.root.clusters.set(config.id, config);

      fileSegment.create(iStartSize);
      overflowSegment.create((iStartSize * 20) / 100);
      overflowStatistic.create(-1);

      isOpen = true;

      allocateSpace((int) (mainBucketsSize * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES));

    } finally {
      releaseExclusiveLock();
    }
  }

  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      fileSegment.open();
      overflowSegment.open();
      overflowStatistic.open();

      deserializeState();

      isOpen = true;

      clearCache();
    } finally {
      releaseExclusiveLock();
    }
  }

  private void init(final OStorage iStorage, final int iId, final String iClusterName, final int iDataSegmentId) throws IOException {
    OFileUtils.checkValidName(iClusterName);

    OStorageLocal storage = (OStorageLocal) iStorage;
    config.setDataSegmentId(iDataSegmentId);
    config.id = iId;
    config.name = iClusterName;
    name = iClusterName;
    id = iId;

    if (fileSegment == null) {
      fileSegment = new OMultiFileSegment(storage, config, DEF_EXTENSION, OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

      config.setOverflowFile(new OStorageClusterLocalLHPEOverflowConfiguration(storage.getConfiguration(), config.name, iId));

      config.setOverflowStatisticsFile(new OStorageClusterLocalLHPEStatisticConfiguration(config,
          OStorageVariableParser.DB_PATH_VARIABLE + '/' + config.name, config.fileType, config.fileMaxSize));

      overflowSegment = new OClusterLocalLHPEOverflow(storage, config.getOverflowSegment(), this,
          OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);
      overflowStatistic = new OClusterLocalLHPEStatistic(storage, config.getOverflowStatisticsFile());
    }
  }

  public void close() throws IOException {
    acquireExclusiveLock();
    try {
      if (!isOpen)
        return;

      serializeState();

      fileSegment.close();
      overflowSegment.close();
      overflowStatistic.close();
      isOpen = false;

    } finally {
      releaseExclusiveLock();
    }

  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {

      truncate();
      for (OFile f : fileSegment.files)
        f.delete();

      fileSegment.files = null;
      overflowSegment.delete();
      overflowStatistic.delete();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    acquireExclusiveLock();
    try {

      switch (iAttribute) {
      case NAME:
        setNameInternal(stringValue);
        break;
      case DATASEGMENT:
        setDataSegmentInternal(stringValue);
        break;
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  private void setNameInternal(final String iNewName) {
    if (storage.getClusterIdByName(iNewName) > -1)
      throw new IllegalArgumentException("Cluster with name '" + iNewName + "' already exists");

    for (int i = 0; i < fileSegment.files.length; i++) {
      final String osFileName = fileSegment.files[i].getName();
      if (osFileName.startsWith(name)) {
        final File newFile = new File(storage.getStoragePath() + '/' + iNewName
            + osFileName.substring(osFileName.lastIndexOf(name) + name.length()));
        for (OStorageFileConfiguration conf : config.infoFiles) {
          if (conf.parent.name.equals(name))
            conf.parent.name = iNewName;
          if (conf.path.endsWith(osFileName))
            conf.path = conf.path.replace(osFileName, newFile.getName());
        }
        boolean renamed = fileSegment.files[i].renameTo(newFile);
        while (!renamed) {
          OMemoryWatchDog.freeMemory(100);
          renamed = fileSegment.files[i].renameTo(newFile);
        }
      }
    }
    config.name = iNewName;

    for (int i = 0; i < overflowSegment.files.length; i++) {
      final String osFileName = overflowSegment.files[i].getName();
      if (osFileName.startsWith(name)) {
        final File newFile = new File(storage.getStoragePath() + '/' + iNewName
            + osFileName.substring(osFileName.lastIndexOf(name) + name.length()));
        for (OStorageFileConfiguration conf : config.infoFiles) {
          if (conf.parent.name.equals(name))
            conf.parent.name = iNewName;
          if (conf.path.endsWith(osFileName))
            conf.path = conf.path.replace(osFileName, newFile.getName());
        }
        boolean renamed = overflowSegment.files[i].renameTo(newFile);
        while (!renamed) {
          OMemoryWatchDog.freeMemory(100);
          renamed = overflowSegment.files[i].renameTo(newFile);
        }
      }
    }
    overflowStatistic.rename(name, iNewName);

    storage.renameCluster(name, iNewName);
    name = iNewName;
    storage.getConfiguration().update();
  }

  /**
   * Assigns a different data-segment id.
   * 
   * @param iName
   *          Data-segment's name
   */
  private void setDataSegmentInternal(final String iName) {
    final int dataId = storage.getDataSegmentIdByName(iName);
    config.setDataSegmentId(dataId);
    storage.getConfiguration().update();
  }

  private void initState() {
    lastOverflowBucket = -1;
    recordSplitPointer = 0;

    g = GROUP_COUNT;
    d = INITIAL_D;

    pageSize = 1 << d;
    nextPageSize = pageSize << 1;

    roundCapacity = pageSize;

    size = 0;
    mainBucketsSize = nextPageSize;

    splittedBuckets = new BitSet((int) pageSize);

    mainBucketOverflowInfoByIndex = new HashMap<Long, Integer>(1024);
    groupBucketOverflowInfoByIndex = new HashMap<Long, Integer>(1024);
    groupBucketOverflowInfoByChainLength = new HashMap[16];
    maxChainIndex = 0;
  }

  public void truncate() throws IOException {
    acquireExclusiveLock();
    try {
      long localSize = size;
      long position = 0;

      while (localSize > 0) {
        OClusterLocalLHPEBucket bucket = loadMainBucket(position);

        while (true) {
          for (int n = 0; n < bucket.getSize(); n++) {
            final OPhysicalPosition ppos = bucket.getPhysicalPosition(n);
            if (storage.checkForRecordValidity(ppos)) {
              storage.getDataSegmentById(ppos.dataSegmentId).deleteRecord(ppos.dataSegmentPos);
              localSize--;
            }
          }
          if (bucket.getOverflowBucket() < 0)
            break;

          bucket = loadOverflowBucket(bucket.getOverflowBucket());
        }
        position++;
      }

      fileSegment.truncate();
      overflowSegment.truncate();
      overflowStatistic.truncate();

      initState();
      allocateSpace((int) (mainBucketsSize * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES));

    } finally {
      releaseExclusiveLock();
    }
  }

  public int getDataSegmentId() {
    acquireSharedLock();
    try {
      return config.getDataSegmentId();
    } finally {
      releaseSharedLock();
    }
  }

  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    acquireExclusiveLock();
    try {
      long[] pos = calculatePageIndex(iPPosition.clusterPosition);
      long position = pos[0];
      final long offset = pos[1];

      final int prevChainLength = getMainBucketOverflowChainLength(position);

      int chainLength = 0;

      OClusterLocalLHPEBucket currentBucket = loadMainBucket(position);

      while (true) {
        for (int i = 0; i < currentBucket.getSize(); i++) {
          long bucketKey = currentBucket.getKey(i);

          if (bucketKey == iPPosition.clusterPosition)
            return false;
        }

        if (currentBucket.getOverflowBucket() > -1) {
          currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
          chainLength++;
        } else
          break;
      }

      iPPosition.recordVersion = 0;
      if (currentBucket.getSize() < OClusterLocalLHPEBucket.BUCKET_CAPACITY)
        currentBucket.addPhysicalPosition(iPPosition);
      else {
        final OverflowBucketInfo bucketInfo = popOverflowBucket();

        final OClusterLocalLHPEBucket overflowBucket = bucketInfo.bucket;

        currentBucket.setOverflowBucket(bucketInfo.index);
        overflowBucket.addPhysicalPosition(iPPosition);

        chainLength++;
        updateMainBucketOverflowChainLength(position, chainLength);
        updateBucketGroupOverflowChainLength(offset, chainLength - prevChainLength);
      }

      size++;

      splitBucketIfNeeded();

      storeBuckets();

      return true;
    } finally {
      clearCache();
      releaseExclusiveLock();
    }
  }

  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    acquireSharedLock();
    try {
      final BucketInfo bucketInfo = findBucket(iPPosition.clusterPosition);
      if (bucketInfo == null)
        return null;

      return bucketInfo.bucket.getPhysicalPosition(bucketInfo.index);
    } finally {
      clearCache();
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] getPositionsByEntryPos(long entryPosition) throws IOException {
    acquireSharedLock();
    try {
      if (entryPosition < 0 || entryPosition > mainBucketsSize - 1)
        return new OPhysicalPosition[0];

      OClusterLocalLHPEBucket bucket = loadMainBucket(entryPosition);

      int sum = 0;
      while (true) {
        sum += bucket.getSize();

        if (bucket.getOverflowBucket() > -1)
          bucket = loadOverflowBucket(bucket.getOverflowBucket());
        else
          break;
      }

      OPhysicalPosition[] result = new OPhysicalPosition[sum];
      int pos = 0;
      bucket = loadMainBucket(entryPosition);

      while (true) {
        for (int i = 0; i < bucket.getSize(); i++) {
          result[pos] = bucket.getPhysicalPosition(i);
          pos++;
        }

        if (bucket.getOverflowBucket() > -1)
          bucket = loadOverflowBucket(bucket.getOverflowBucket());
        else
          break;
      }

      return result;

    } finally {
      clearCache();
      releaseSharedLock();
    }
  }

  public void updateDataSegmentPosition(long iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
    acquireExclusiveLock();
    try {
      final BucketInfo bucketInfo = findBucket(iPosition);

      if (!bucketInfo.bucket.isOverflowBucket()) {
        final int idOffset = OClusterLocalLHPEBucket.getDataSegmentIdOffset(bucketInfo.index);

        final long filePos = bucketInfo.bucket.getFilePosition() + idOffset;

        final byte[] serializedDataSegment = OClusterLocalLHPEBucket.serializeDataSegmentId(iDataSegmentId);
        final byte[] serializedDataPosition = OClusterLocalLHPEBucket.serializeDataPosition(iDataPosition);

        fileSegment.writeContinuously(filePos, serializedDataSegment);
        fileSegment.writeContinuously(filePos + serializedDataSegment.length, serializedDataPosition);
      } else
        overflowSegment.updateDataSegmentPosition(bucketInfo.bucket, bucketInfo.index, iDataSegmentId, iDataPosition);
    } finally {
      clearCache();
      releaseExclusiveLock();
    }
  }

  public void removePhysicalPosition(long iPosition) throws IOException {
    acquireExclusiveLock();
    try {
      final long[] pos = calculatePageIndex(iPosition);
      long position = pos[0];
      final long offset = pos[1];

      OClusterLocalLHPEBucket mainBucket = loadMainBucket(position);
      OClusterLocalLHPEBucket currentBucket = mainBucket;

      while (true) {
        for (int i = 0; i < currentBucket.getSize(); i++) {
          long bucketKey = currentBucket.getKey(i);

          if (bucketKey == iPosition) {
            currentBucket.removePhysicalPosition(i);

            size--;
            mergeBucketsIfNeeded();
            compressChain(mainBucket, position, offset);

            break;
          }
        }

        if (currentBucket.getOverflowBucket() > -1)
          currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
        else
          break;
      }

      storeBuckets();
    } finally {
      clearCache();
      releaseExclusiveLock();
    }
  }

  public void updateRecordType(long iPosition, byte iRecordType) throws IOException {
    acquireExclusiveLock();
    try {
      final BucketInfo bucketInfo = findBucket(iPosition);

      if (!bucketInfo.bucket.isOverflowBucket()) {
        final int recordTypeOffset = OClusterLocalLHPEBucket.getRecordTypeOffset(bucketInfo.index);

        final long filePos = bucketInfo.bucket.getFilePosition() + recordTypeOffset;

        final long[] pos = fileSegment.getRelativePosition(filePos);

        final OFile file = fileSegment.files[(int) pos[0]];
        long p = pos[1];

        file.writeByte(p, iRecordType);
      } else
        overflowSegment.updateRecordType(bucketInfo.bucket, bucketInfo.index, iRecordType);

    } finally {
      clearCache();
      releaseExclusiveLock();
    }
  }

  public void updateVersion(long iPosition, int iVersion) throws IOException {
    acquireExclusiveLock();
    try {
      final BucketInfo bucketInfo = findBucket(iPosition);

      if (!bucketInfo.bucket.isOverflowBucket()) {
        final int versionOffset = OClusterLocalLHPEBucket.getVersionOffset(bucketInfo.index);
        final byte[] serializedVersion = OClusterLocalLHPEBucket.serializeVersion(iVersion);

        final long filePos = bucketInfo.bucket.getFilePosition() + versionOffset;

        fileSegment.writeContinuously(filePos, serializedVersion);
      } else
        overflowSegment.updateVersion(bucketInfo.bucket, bucketInfo.index, iVersion);
    } finally {
      clearCache();
      releaseExclusiveLock();
    }
  }

  public long getEntries() {
    acquireSharedLock();
    try {
      return size;
    } finally {
      releaseSharedLock();
    }
  }

  public long getFirstEntryPosition() {
    acquireSharedLock();
    try {
      if (size == 0)
        return -1;
      return 0;
    } finally {
      releaseSharedLock();
    }
  }

  public long getLastEntryPosition() {
    acquireSharedLock();
    try {
      if (size == 0)
        return -1;
      return mainBucketsSize - 1;
    } finally {
      releaseSharedLock();
    }
  }

  public void lock() {
    acquireSharedLock();
  }

  public void unlock() {
    releaseSharedLock();
  }

  public String getType() {
    return TYPE;
  }

  public int getId() {
    return id;
  }

  public void synch() throws IOException {
    acquireSharedLock();
    try {

      serializeState();

      fileSegment.synch();
      overflowSegment.synch();
      overflowStatistic.synch();

    } finally {
      releaseSharedLock();
    }
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {

      fileSegment.setSoftlyClosed(softlyClosed);
      overflowSegment.setSoftlyClosed(softlyClosed);
      overflowStatistic.setSoftlyClosed(softlyClosed);

    } finally {
      releaseExclusiveLock();
    }
  }

  public String getName() {
    return name;
  }

  public boolean generatePositionBeforeCreation() {
    return true;
  }

  public long getRecordsSize() {
    acquireSharedLock();
    try {
      long calculatedSize = fileSegment.getFilledUpTo();

      long localSize = size;
      long position = 0;

      while (localSize > 0) {
        OClusterLocalLHPEBucket bucket = loadMainBucket(position);

        while (true) {
          for (int n = 0; n < bucket.getSize(); n++) {
            OPhysicalPosition ppos = bucket.getPhysicalPosition(n);
            if (ppos.dataSegmentPos > -1 && ppos.recordVersion > -1)
              calculatedSize += storage.getDataSegmentById(ppos.dataSegmentId).getRecordSize(ppos.dataSegmentPos);
            localSize--;
          }
          if (bucket.getOverflowBucket() < 0)
            break;

          bucket = loadOverflowBucket(bucket.getOverflowBucket());
        }
        position++;
      }
      return calculatedSize;
    } catch (IOException e) {
      throw new OIOException("Error on calculating cluster size for: " + name, e);
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterEntryIterator absoluteIterator() {
    return new OClusterEntryIterator(this);
  }

  private long calcPositionToMerge() {
    return mainBucketsSize - pageSize * g - 1;
  }

  private void mergeBucketsIfNeeded() throws IOException {
    if (mainBucketsSize <= 2)
      return;

    final double currentCapacity = 1.0 * size / (mainBucketsSize * OClusterLocalLHPEBucket.BUCKET_CAPACITY);
    if (currentCapacity < storageMergeOverflow)
      mergeBuckets();
  }

  private void mergeBuckets() throws IOException {
    long positionToMerge = calcPositionToMerge();
    if (positionToMerge < 0) {
      g--;
      if (g < GROUP_COUNT) {
        g = 2 * GROUP_COUNT - 1;

        nextPageSize = pageSize;
        pageSize = pageSize >>> 1;

        roundCapacity = pageSize;

        splittedBuckets = new BitSet((int) pageSize);

      }

      splittedBuckets.set(0, (int) pageSize);

      recordSplitPointer = roundCapacity;

      rebuildGroupOverflowChain();

      positionToMerge = calcPositionToMerge();
    }

    splittedBuckets.clear((int) positionToMerge);

    Map<Long, OClusterLocalLHPEBucket> bucketMap = new HashMap<Long, OClusterLocalLHPEBucket>(g);
    List<Long> bucketsToMerge = new ArrayList<Long>(g);

    for (long ptr = positionToMerge; ptr <= positionToMerge + pageSize * g; ptr += pageSize) {
      final OClusterLocalLHPEBucket bucket = loadMainBucket(ptr);
      bucketMap.put(ptr, bucket);

      bucketsToMerge.add(ptr);
    }

    for (long currentBucketPosition : bucketsToMerge) {
      OClusterLocalLHPEBucket currentBucket = bucketMap.get(currentBucketPosition);
      while (true) {
        for (int i = 0; i < currentBucket.getSize();) {
          long bucketKey = currentBucket.getKey(i);

          long position = calculatePageIndex(bucketKey)[0];
          OClusterLocalLHPEBucket bucketToAdd = bucketMap.get(position);

          if (currentBucketPosition != position) {
            while (bucketToAdd.getSize() >= OClusterLocalLHPEBucket.BUCKET_CAPACITY && bucketToAdd.getOverflowBucket() > -1)
              bucketToAdd = loadOverflowBucket(bucketToAdd.getOverflowBucket());

            if (bucketToAdd.getSize() >= OClusterLocalLHPEBucket.BUCKET_CAPACITY) {
              OverflowBucketInfo overflowBucket = popOverflowBucket();

              bucketToAdd.setOverflowBucket(overflowBucket.index);
              bucketToAdd = overflowBucket.bucket;
            }

            bucketToAdd.addPhysicalPosition(currentBucket.getPhysicalPosition(i));

            currentBucket.removePhysicalPosition(i);
          } else
            i++;
        }

        if (currentBucket.getOverflowBucket() > -1)
          currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
        else
          break;
      }
    }

    for (long currentBucketPosition : bucketsToMerge) {
      final OClusterLocalLHPEBucket currentBucket = bucketMap.get(currentBucketPosition);

      compressChain(currentBucket, currentBucketPosition, positionToMerge);
    }

    recordSplitPointer = splittedBuckets.nextClearBit(0);

    OClusterLocalLHPEBucket bucketToMerge = bucketMap.get(bucketsToMerge.get(bucketsToMerge.size() - 1));
    mainBucketsSize--;

    mainBucketsToStore.remove(bucketToMerge);

    final byte[] empty = new byte[OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES];

    final long filePos = bucketToMerge.getFilePosition();

    fileSegment.writeContinuously(filePos, empty);
  }

  private void clearCache() {
    mainBucketCache.get().clear();
    overflowBucketCache.get().clear();
  }

  private OClusterLocalLHPEBucket loadMainBucket(long position) throws IOException {
    final Map<Long, OClusterLocalLHPEBucket> bucketCache = mainBucketCache.get();

    OClusterLocalLHPEBucket clusterBucket = bucketCache.get(position);
    if (clusterBucket != null)
      return clusterBucket;

    final long filePos = position * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;

    final byte[] buffer = new byte[OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES];
    fileSegment.readContinuously(filePos, buffer, OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES);

    clusterBucket = new OClusterLocalLHPEBucket(buffer, this, filePos, false);

    bucketCache.put(position, clusterBucket);

    return clusterBucket;
  }

  private OClusterLocalLHPEBucket loadOverflowBucket(long position) throws IOException {
    final Map<Long, OClusterLocalLHPEBucket> bucketCache = overflowBucketCache.get();

    OClusterLocalLHPEBucket clusterBucket = bucketCache.get(position);
    if (clusterBucket != null)
      return clusterBucket;

    final long filePos = position * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;

    clusterBucket = overflowSegment.loadBucket(filePos);

    bucketCache.put(position, clusterBucket);

    return clusterBucket;
  }

  private void storeBuckets() throws IOException {
    for (OClusterLocalLHPEBucket mainBucket : mainBucketsToStore) {
      final long filledUpTo = fileSegment.getFilledUpTo();
      final long endPos = mainBucket.getFilePosition() + OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;
      if (endPos > filledUpTo)
        allocateSpace((int) (endPos - filledUpTo));

      mainBucket.serialize();

      final long filePos = mainBucket.getFilePosition();

      fileSegment.writeContinuously(filePos, mainBucket.getBuffer());
    }

    mainBucketsToStore.clear();

    for (OClusterLocalLHPEBucket overflowBucket : overflowBucketsToStore) {
      overflowBucket.serialize();
      overflowSegment.updateBucket(overflowBucket);
    }

    overflowBucketsToStore.clear();
  }

  private void splitBucketIfNeeded() throws IOException {
    double currentCapacity = 1.0 * size / (mainBucketsSize * OClusterLocalLHPEBucket.BUCKET_CAPACITY);
    if (currentCapacity > storageSplitOverflow)
      splitBuckets();
  }

  private void splitBuckets() throws IOException {
    final long positionToSplit = calcPositionToSplit();
    splittedBuckets.set((int) positionToSplit);

    final long positionToAdd = positionToSplit + pageSize * g;

    if (mainBucketsSize - 1 < positionToAdd) {
      final long requiredSpace = (positionToAdd + 1) * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES;
      if (requiredSpace > fileSegment.getFilledUpTo())
        allocateSpace((int) (requiredSpace - fileSegment.getFilledUpTo()));

      mainBucketsSize = positionToAdd + 1;
    }

    // Empty bucket
    fileSegment.writeContinuously(positionToAdd * OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES,
        new byte[OClusterLocalLHPEBucket.BUCKET_SIZE_IN_BYTES]);

    // TODO discuss if we need load main bucket or just empty file and construct new bucket
    OClusterLocalLHPEBucket addedBucket = loadMainBucket(positionToAdd);

    Map<Long, OClusterLocalLHPEBucket> bucketMap = new HashMap<Long, OClusterLocalLHPEBucket>(g);
    List<Long> bucketsToSplit = new ArrayList<Long>(g);

    for (long ptr = positionToSplit; ptr < positionToSplit + pageSize * g; ptr += pageSize) {
      final OClusterLocalLHPEBucket bucket = loadMainBucket(ptr);
      bucketMap.put(ptr, bucket);

      bucketsToSplit.add(ptr);
    }

    bucketMap.put(positionToAdd, addedBucket);

    int chainLength = 0;
    int prevChainLength = getMainBucketOverflowChainLength(positionToAdd);

    for (long currentBucketPosition : bucketsToSplit) {
      OClusterLocalLHPEBucket currentBucket = bucketMap.get(currentBucketPosition);
      while (true) {
        for (int i = 0; i < currentBucket.getSize();) {
          long bucketKey = currentBucket.getKey(i);

          long position = calculatePageIndex(bucketKey)[0];
          if (currentBucketPosition != position) {
            OClusterLocalLHPEBucket bucketToAdd = bucketMap.get(position);

            while (bucketToAdd.getSize() >= OClusterLocalLHPEBucket.BUCKET_CAPACITY && bucketToAdd.getOverflowBucket() > -1)
              bucketToAdd = loadOverflowBucket(bucketToAdd.getOverflowBucket());

            if (bucketToAdd.getSize() >= OClusterLocalLHPEBucket.BUCKET_CAPACITY) {
              OverflowBucketInfo bucketInfo = popOverflowBucket();

              bucketToAdd.setOverflowBucket(bucketInfo.index);
              bucketToAdd = bucketInfo.bucket;

              if (position == positionToAdd)
                chainLength++;
            }

            bucketToAdd.addPhysicalPosition(currentBucket.getPhysicalPosition(i));

            currentBucket.removePhysicalPosition(i);
          } else
            i++;
        }

        if (currentBucket.getOverflowBucket() > -1)
          currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
        else
          break;
      }
    }

    updateMainBucketOverflowChainLength(positionToAdd, chainLength);
    updateBucketGroupOverflowChainLength(positionToSplit, chainLength - prevChainLength);

    for (long currentBucketPosition : bucketsToSplit) {
      final OClusterLocalLHPEBucket currentBucket = bucketMap.get(currentBucketPosition);
      compressChain(currentBucket, currentBucketPosition, positionToSplit);
    }

    recordSplitPointer = splittedBuckets.nextClearBit((int) recordSplitPointer);

    if (recordSplitPointer == roundCapacity) {
      recordSplitPointer = 0;

      splittedBuckets.clear();

      g++;

      if (g == 2 * GROUP_COUNT) {
        roundCapacity = nextPageSize;
        pageSize = nextPageSize;
        nextPageSize <<= 1;
        g = GROUP_COUNT;

        splittedBuckets = new BitSet((int) pageSize);
      }

      rebuildGroupOverflowChain();
    }
  }

  private BucketInfo findBucket(long clusterPosition) throws IOException {
    long position = calculatePageIndex(clusterPosition)[0];

    OClusterLocalLHPEBucket currentBucket = loadMainBucket(position);

    while (true) {
      for (int i = 0; i < currentBucket.getSize(); i++) {
        long bucketKey = currentBucket.getKey(i);
        if (bucketKey == clusterPosition) {
          final BucketInfo bucketInfo = new BucketInfo();
          bucketInfo.index = i;
          bucketInfo.bucket = currentBucket;

          return bucketInfo;
        }
      }

      if (currentBucket.getOverflowBucket() > -1)
        currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
      else
        return null;
    }
  }

  private void compressChain(OClusterLocalLHPEBucket mainBucket, long index, long offset) throws IOException {
    OClusterLocalLHPEBucket currentBucket = mainBucket;
    while (true) {
      if (currentBucket.getOverflowBucket() < 0)
        break;

      int diff = OClusterLocalLHPEBucket.BUCKET_CAPACITY - currentBucket.getSize();

      OClusterLocalLHPEBucket nextBucket = loadOverflowBucket(currentBucket.getOverflowBucket());

      while (nextBucket.getSize() == 0) {
        if (nextBucket.getOverflowBucket() > -1)
          nextBucket = loadOverflowBucket(nextBucket.getOverflowBucket());
        else
          break;
      }

      if (nextBucket.getSize() > 0)
        for (int i = 0; i < diff; i++) {
          final OPhysicalPosition physicalPosition = nextBucket.getPhysicalPosition(nextBucket.getSize() - 1);

          currentBucket.addPhysicalPosition(physicalPosition);
          nextBucket.removePhysicalPosition(nextBucket.getSize() - 1);

          while (nextBucket.getSize() == 0) {
            if (nextBucket.getOverflowBucket() > -1)
              nextBucket = loadOverflowBucket(nextBucket.getOverflowBucket());
            else
              break;
          }

          if (nextBucket.getSize() == 0)
            break;
        }

      if (currentBucket.getSize() < OClusterLocalLHPEBucket.BUCKET_CAPACITY) {
        break;
      }

      if (currentBucket.getOverflowBucket() > -1)
        currentBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
      else {
        break;
      }
    }

    int chainLength = 0;

    final int prevChainLength = getMainBucketOverflowChainLength(index);

    currentBucket = mainBucket;
    while (true) {
      if (currentBucket.getOverflowBucket() < 0)
        break;

      final OClusterLocalLHPEBucket nextBucket = loadOverflowBucket(currentBucket.getOverflowBucket());
      if (nextBucket.getSize() == 0) {
        putBucketToOverflowList(nextBucket, currentBucket.getOverflowBucket());

        currentBucket.setOverflowBucket(-1);
        break;
      }

      chainLength++;
      currentBucket = nextBucket;
    }

    updateMainBucketOverflowChainLength(index, chainLength);
    updateBucketGroupOverflowChainLength(offset, chainLength - prevChainLength);

  }

  private void putBucketToOverflowList(OClusterLocalLHPEBucket bucket, long index) throws IOException {
    OClusterLocalLHPEBucket currentBucket = bucket;

    while (true) {
      long nextBucket = currentBucket.getOverflowBucket();

      currentBucket.setOverflowBucket(lastOverflowBucket);
      lastOverflowBucket = index;

      if (nextBucket == -1)
        break;

      currentBucket = loadOverflowBucket(nextBucket);
      index = nextBucket;
    }
  }

  private OverflowBucketInfo popOverflowBucket() throws IOException {
    OverflowBucketInfo result = new OverflowBucketInfo();

    if (lastOverflowBucket > -1) {
      result.bucket = loadOverflowBucket(lastOverflowBucket);

      result.index = lastOverflowBucket;

      lastOverflowBucket = result.bucket.getOverflowBucket();

      result.bucket.setOverflowBucket(-1);
    } else {
      OClusterLocalLHPEBucket bucket = overflowSegment.createBucket();
      result.index = overflowSegment.getBucketsSize() - 1;

      result.bucket = bucket;

      overflowBucketCache.get().put(result.index, bucket);
    }

    return result;

  }

  void addToMainStoreList(final OClusterLocalLHPEBucket bucket) {
    mainBucketsToStore.add(bucket);
  }

  void addToOverflowStoreList(final OClusterLocalLHPEBucket bucket) {
    overflowBucketsToStore.add(bucket);
  }

  private long[] calculatePageIndex(long key) {
    long[] result = new long[2];

    long ps = pageSize;

    long offset = calculateOffset(key, ps);
    long group;

    if (!splittedBuckets.get((int) offset))
      group = calculateGroup(key, g);
    else if (g != 2 * GROUP_COUNT - 1)
      group = calculateGroup(key, g + 1);
    else {
      ps = nextPageSize;
      offset = calculateOffset(key, ps);
      group = calculateGroup(key, GROUP_COUNT);
    }

    result[0] = ps * group + offset;
    result[1] = offset;
    return result;
  }

  private long calcPositionToSplit() {
    if (maxChainIndex < 1)
      return recordSplitPointer;
    else {
      for (int i = maxChainIndex; i >= 1; i--) {
        final Map<Long, Integer> infoMap = groupBucketOverflowInfoByChainLength[i];

        if (infoMap == null || infoMap.isEmpty())
          continue;

        for (Long index : infoMap.keySet()) {
          if (!splittedBuckets.get(index.intValue())) {
            return index;
          }
        }
      }
    }

    return recordSplitPointer;
  }

  private int getMainBucketOverflowChainLength(long bucketIndex) {
    final Integer mainBucketOverflowInfo = mainBucketOverflowInfoByIndex.get(bucketIndex);
    if (mainBucketOverflowInfo == null)
      return 0;

    return mainBucketOverflowInfo;
  }

  private void updateBucketGroupOverflowChainLength(long groupIndex, int diff) {
    if (diff == 0)
      return;

    Integer mainBucketOverflowInfo = groupBucketOverflowInfoByIndex.get(groupIndex);

    int prevChainLength = 0;
    if (mainBucketOverflowInfo == null) {
      mainBucketOverflowInfo = diff;
      groupBucketOverflowInfoByIndex.put(groupIndex, mainBucketOverflowInfo);
    } else {
      prevChainLength = mainBucketOverflowInfo;

      mainBucketOverflowInfo += diff;

      if (mainBucketOverflowInfo == 0)
        groupBucketOverflowInfoByIndex.remove(groupIndex);
      else
        groupBucketOverflowInfoByIndex.put(groupIndex, mainBucketOverflowInfo);
    }

    Map<Long, Integer> prevChainMap = null;
    if (prevChainLength > 0)
      prevChainMap = groupBucketOverflowInfoByChainLength[prevChainLength];

    if (mainBucketOverflowInfo > 0) {
      if (mainBucketOverflowInfo == groupBucketOverflowInfoByChainLength.length) {
        final Map<Long, Integer>[] newGroupBucketOverflow = new HashMap[groupBucketOverflowInfoByChainLength.length << 1];
        System.arraycopy(groupBucketOverflowInfoByChainLength, 0, newGroupBucketOverflow, 0,
            groupBucketOverflowInfoByChainLength.length);
        groupBucketOverflowInfoByChainLength = newGroupBucketOverflow;
      }

      Map<Long, Integer> nextChainMap = groupBucketOverflowInfoByChainLength[mainBucketOverflowInfo];
      if (nextChainMap == null) {
        nextChainMap = new HashMap<Long, Integer>(1024);
        groupBucketOverflowInfoByChainLength[mainBucketOverflowInfo] = nextChainMap;
      }

      nextChainMap.put(groupIndex, mainBucketOverflowInfo);
    }

    if (mainBucketOverflowInfo > maxChainIndex)
      maxChainIndex = mainBucketOverflowInfo;

    if (prevChainMap != null)
      prevChainMap.remove(groupIndex);

    if (prevChainLength == maxChainIndex) {
      while (maxChainIndex >= 0
          && (groupBucketOverflowInfoByChainLength[maxChainIndex] == null || groupBucketOverflowInfoByChainLength[maxChainIndex]
              .isEmpty()))
        maxChainIndex--;
    }

  }

  private void updateMainBucketOverflowChainLength(long mainBucketIndex, int val) {
    Integer mainBucketOverflowInfo = mainBucketOverflowInfoByIndex.get(mainBucketIndex);

    if (mainBucketOverflowInfo == null) {
      if (val == 0)
        return;

      mainBucketOverflowInfoByIndex.put(mainBucketIndex, val);
    } else {
      if (mainBucketOverflowInfo == val)
        return;

      if (val == 0)
        mainBucketOverflowInfoByIndex.remove(mainBucketIndex);
      else
        mainBucketOverflowInfoByIndex.put(mainBucketIndex, val);
    }
  }

  private void rebuildGroupOverflowChain() {
    groupBucketOverflowInfoByChainLength = new HashMap[16];
    groupBucketOverflowInfoByIndex.clear();

    maxChainIndex = -1;

    for (long i = 0; i < pageSize; i++) {
      int chainLength = 0;

      for (int cg = 0; cg < g; cg++) {
        final Integer mainBucketOverflowInfo = mainBucketOverflowInfoByIndex.get(cg * pageSize + i);
        if (mainBucketOverflowInfo != null)
          chainLength += mainBucketOverflowInfo;
      }

      if (chainLength > 0) {
        if (chainLength > groupBucketOverflowInfoByChainLength.length) {
          final Map<Long, Integer>[] newGroupBucketOverflow = new HashMap[groupBucketOverflowInfoByChainLength.length << 1];
          System.arraycopy(groupBucketOverflowInfoByChainLength, 0, newGroupBucketOverflow, 0,
              groupBucketOverflowInfoByChainLength.length);
          groupBucketOverflowInfoByChainLength = newGroupBucketOverflow;
        }

        Map<Long, Integer> infoMap = groupBucketOverflowInfoByChainLength[chainLength];
        if (infoMap == null) {
          infoMap = new HashMap<Long, Integer>(1024);
          groupBucketOverflowInfoByChainLength[chainLength] = infoMap;
        }

        infoMap.put(i, chainLength);

        groupBucketOverflowInfoByIndex.put(i, chainLength);

        if (chainLength > maxChainIndex)
          maxChainIndex = chainLength;
      }
    }
  }

  private static long calculateGroup(long key, int gValue) {
    if (gValue == 2)
      return key & 1;

    if (gValue == 3)
      return key % 3;

    throw new IllegalStateException("Invalid group value, should be 2 or 3 but is " + gValue);
  }

  private static long calculateOffset(long key, long pageSize) {
    return key & (pageSize - 1);
  }

  private void serializeState() throws IOException {
    final OFile file = fileSegment.files[0];

    int pos = 0;
    file.writeHeaderLong(0, lastOverflowBucket);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, recordSplitPointer);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, roundCapacity);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, g);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, d);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, pageSize);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, nextPageSize);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, size);
    pos += OLongSerializer.LONG_SIZE;

    file.writeHeaderLong(pos, mainBucketsSize);

    OMemoryStream byteArrayOutputStream = new OMemoryStream(splittedBuckets.size() / 8);
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(splittedBuckets);

    byte[] serializedBitSet = byteArrayOutputStream.toByteArray();

    objectOutputStream.flush();
    objectOutputStream.close();

    objectOutputStream = null;
    byteArrayOutputStream = null;

    final int statisticSize = (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE) * mainBucketOverflowInfoByIndex.size()
        + serializedBitSet.length + 2 * OIntegerSerializer.INT_SIZE;

    overflowStatistic.truncate();

    final OFile statisticsFile = overflowStatistic.file;
    statisticsFile.allocateSpace(statisticSize);

    pos = 0;
    statisticsFile.writeInt(pos, serializedBitSet.length);
    pos += OIntegerSerializer.INT_SIZE;

    statisticsFile.write(pos, serializedBitSet);
    pos += serializedBitSet.length;

    serializedBitSet = null;

    statisticsFile.writeInt(pos, mainBucketOverflowInfoByIndex.size());

    for (Map.Entry<Long, Integer> statisticEntry : mainBucketOverflowInfoByIndex.entrySet()) {
      statisticsFile.writeLong(pos, statisticEntry.getKey());
      pos += OLongSerializer.LONG_SIZE;

      statisticsFile.writeInt(pos, statisticEntry.getValue());
      pos += OIntegerSerializer.INT_SIZE;
    }
  }

  private void deserializeState() throws IOException {
    final OFile file = fileSegment.files[0];

    int pos = 0;

    lastOverflowBucket = file.readHeaderLong(0);
    pos += OLongSerializer.LONG_SIZE;

    recordSplitPointer = file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    roundCapacity = file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    g = (int) file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    d = (int) file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    pageSize = file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    nextPageSize = (int) file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    size = file.readHeaderLong(pos);
    pos += OLongSerializer.LONG_SIZE;

    mainBucketsSize = file.readHeaderLong(pos);

    final OFile statisticsFile = overflowStatistic.file;
    pos = 0;

    final int serializedBitSetSize = statisticsFile.readInt(pos);
    pos += OIntegerSerializer.INT_SIZE;

    byte[] serializedBitSet = new byte[serializedBitSetSize];
    statisticsFile.read(pos, serializedBitSet, serializedBitSetSize);

    OMemoryInputStream byteArrayInputStream = new OMemoryInputStream(serializedBitSet);
    ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
    try {
      splittedBuckets = (BitSet) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new OStorageException("Error during opening cluster " + name, e);
    } finally {
      objectInputStream.close();
      byteArrayInputStream.close();
    }

    pos += serializedBitSet.length;

    objectInputStream.close();
    objectInputStream = null;
    byteArrayInputStream = null;
    serializedBitSet = null;

    final int mapSize = statisticsFile.readInt(pos);
    pos += OIntegerSerializer.INT_SIZE;

    mainBucketOverflowInfoByIndex = new HashMap<Long, Integer>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      final long key = statisticsFile.readLong(pos);
      pos += OLongSerializer.LONG_SIZE;

      final int value = statisticsFile.readInt(pos);
      pos += OIntegerSerializer.INT_SIZE;

      mainBucketOverflowInfoByIndex.put(key, value);
    }

    rebuildGroupOverflowChain();
  }

  private void allocateSpace(int iSize) throws IOException {
    int remainingSize = iSize;
    final long offset = fileSegment.allocateSpaceContinuously(iSize);

    final long upToPosition = offset + iSize;

    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    while (remainingSize > DEFAULT_BUFFER_SIZE) {
      fileSegment.writeContinuously(upToPosition - remainingSize, buffer);
      remainingSize -= DEFAULT_BUFFER_SIZE;
    }

    fileSegment.writeContinuously(upToPosition - remainingSize, new byte[remainingSize]);
  }

  private static final class BucketInfo {
    private OClusterLocalLHPEBucket bucket;
    int                             index;
  }

  private static final class OverflowBucketInfo {
    private OClusterLocalLHPEBucket bucket;
    long                            index;
  }
}
