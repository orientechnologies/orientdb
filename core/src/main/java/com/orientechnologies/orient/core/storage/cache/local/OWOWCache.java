/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OWriteCacheException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OWOWCache extends OAbstractWriteCache implements OWriteCache, OCachePointer.WritersListener {
  // we add 8 bytes before and after cache pages to prevent word tearing in mt case.

  private final int MAX_PAGES_PER_FLUSH;

  public static final String NAME_ID_MAP_EXTENSION = ".cm";

  private static final String NAME_ID_MAP = "name_id_map" + NAME_ID_MAP_EXTENSION;

  public static final int MIN_CACHE_SIZE = 16;

  public static final long MAGIC_NUMBER = 0xFACB03FEL;

  private static final double MAX_LSN_SEGMENT_DISTANCE_FACTOR = 0.75;

  private final long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024L * 1024L;

  private final int                                        diskSizeCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES
      .getValueAsInteger();
  private final List<WeakReference<OLowDiskSpaceListener>> listeners             = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();

  private final AtomicLong lastDiskSpaceCheck = new AtomicLong(0);
  private final String storagePath;

  private final ConcurrentSkipListMap<PageKey, PageGroup> writeCachePages         = new ConcurrentSkipListMap<PageKey, PageGroup>();
  private final ConcurrentSkipListSet<PageKey>            exclusiveWritePages     = new ConcurrentSkipListSet<PageKey>();
  private final ODistributedCounter                       writeCacheSize          = new ODistributedCounter();
  private final ODistributedCounter                       exclusiveWriteCacheSize = new ODistributedCounter();
  private final ODistributedCounter                       cacheOverflowCount      = new ODistributedCounter();

  private final OBinarySerializer<String>            stringSerializer;
  private final ConcurrentMap<Integer, OFileClassic> files;
  private final boolean                              syncOnPageFlush;
  private final int                                  pageSize;
  private final long                                 groupTTL;
  private final OWriteAheadLog                       writeAheadLog;
  private final AtomicLong amountOfNewPagesAdded = new AtomicLong();

  private final OPartitionedLockManager<PageKey> lockManager = new OPartitionedLockManager<PageKey>();
  private final OLocalPaginatedStorage storageLocal;
  private final OReadersWriterSpinLock filesLock = new OReadersWriterSpinLock();
  private final ScheduledExecutorService commitExecutor;

  private final ExecutorService lowSpaceEventsPublisher;

  private volatile ConcurrentMap<String, Integer> nameIdMap;

  private       RandomAccessFile nameIdMapHolder;
  private final int              writeCacheMaxSize;
  private final int              cacheMaxSize;

  private int fileCounter = 1;

  private PageKey lastPageKey      = new PageKey(0, -1);
  private PageKey lastWritePageKey = new PageKey(0, -1);

  private File nameIdMapHolderFile;

  private final int id;

  private final OPerformanceStatisticManager performanceStatisticManager;

  private final OByteBufferPool bufferPool;

  public OWOWCache(boolean syncOnPageFlush, int pageSize, OByteBufferPool bufferPool, long groupTTL, OWriteAheadLog writeAheadLog,
      long pageFlushInterval, long writeCacheMaxSize, long cacheMaxSize, OLocalPaginatedStorage storageLocal, boolean checkMinSize,
      int id) {
    filesLock.acquireWriteLock();
    try {
      this.id = id;
      this.files = new ConcurrentHashMap<Integer, OFileClassic>();

      this.syncOnPageFlush = syncOnPageFlush;
      this.pageSize = pageSize;
      this.groupTTL = groupTTL;
      this.writeAheadLog = writeAheadLog;
      this.bufferPool = bufferPool;

      int writeNormalizedSize = normalizeMemory(writeCacheMaxSize, pageSize);
      if (checkMinSize && writeNormalizedSize < MIN_CACHE_SIZE)
        writeNormalizedSize = MIN_CACHE_SIZE;

      int normalizedSize = normalizeMemory(cacheMaxSize, pageSize);
      if (checkMinSize && normalizedSize < MIN_CACHE_SIZE)
        normalizedSize = MIN_CACHE_SIZE;

      this.writeCacheMaxSize = writeNormalizedSize;
      this.cacheMaxSize = normalizedSize;

      this.storageLocal = storageLocal;

      this.storagePath = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath());
      this.performanceStatisticManager = storageLocal.getPerformanceStatisticManager();

      final OBinarySerializerFactory binarySerializerFactory = storageLocal.getComponentsFactory().binarySerializerFactory;
      this.stringSerializer = binarySerializerFactory.getObjectSerializer(OType.STRING);

      commitExecutor = Executors.newSingleThreadScheduledExecutor(new FlushThreadFactory(storageLocal.getName()));
      lowSpaceEventsPublisher = Executors.newCachedThreadPool(new LowSpaceEventsPublisherFactory(storageLocal.getName()));

      MAX_PAGES_PER_FLUSH = (int) (4000 / (1000.0 / pageFlushInterval));

      if (pageFlushInterval > 0)
        commitExecutor.scheduleWithFixedDelay(new PeriodicFlushTask(), pageFlushInterval, pageFlushInterval, TimeUnit.MILLISECONDS);

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private int normalizeMemory(final long maxSize, final int pageSize) {
    final long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  /**
   * Directory which contains all files managed by write cache.
   *
   * @return Directory which contains all files managed by write cache or <code>null</code> in case of in memory database.
   */
  @Override
  public File getRootDirectory() {
    return new File(storagePath);
  }

  @Override
  public OPerformanceStatisticManager getPerformanceStatisticManager() {
    return performanceStatisticManager;
  }

  public void startFuzzyCheckpoints() {
    if (writeAheadLog != null) {
      final long fuzzyCheckPointInterval = OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger();
      commitExecutor.scheduleWithFixedDelay(new PeriodicalFuzzyCheckpointTask(), fuzzyCheckPointInterval, fuzzyCheckPointInterval,
          TimeUnit.SECONDS);
    }
  }

  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    listeners.add(new WeakReference<OLowDiskSpaceListener>(listener));
  }

  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    final List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<WeakReference<OLowDiskSpaceListener>>();

    for (WeakReference<OLowDiskSpaceListener> ref : listeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OLowDiskSpaceListener> ref : itemsToRemove)
      listeners.remove(ref);
  }

  private void freeSpaceCheckAfterNewPageAdd() {
    final long newPagesAdded = amountOfNewPagesAdded.incrementAndGet();
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (newPagesAdded - lastSpaceCheck > diskSizeCheckInterval || lastSpaceCheck == 0) {
      final File storageDir = new File(storagePath);

      final long freeSpace = storageDir.getFreeSpace();

      if (freeSpace < freeSpaceLimit)
        callLowSpaceListeners(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));

      lastDiskSpaceCheck.lazySet(newPagesAdded);
    }
  }

  private void callLowSpaceListeners(final OLowDiskSpaceInformation information) {
    lowSpaceEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (WeakReference<OLowDiskSpaceListener> lowDiskSpaceListenerWeakReference : listeners) {
          final OLowDiskSpaceListener listener = lowDiskSpaceListenerWeakReference.get();
          if (listener != null)
            try {
              listener.lowDiskSpace(information);
            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "Error during notification of low disk space for storage " + storageLocal.getName(), e);
            }
        }
      }
    });
  }

  private static int calculatePageCrc(byte[] pageData) {
    final int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  public long bookFileId(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();
      final Integer fileId = nameIdMap.get(fileName);

      if (fileId != null && fileId < 0) {
        return composeFileId(id, -fileId);
      }

      ++fileCounter;

      return composeFileId(id, fileCounter);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public int pageSize() {
    return pageSize;
  }

  @Override
  public boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  public long openFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      if (fileId == null || fileId < 0)
        fileClassic = null;
      else
        fileClassic = files.get(fileId);

      if (fileClassic == null) {
        fileClassic = createFile(fileName);
        if (!fileClassic.exists())
          throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageLocal.getName());
        else {
          // throw new OStorageException("File '" + fileName
          // + "' is not registered in 'file name - id' map, but exists in file system");

          // REGISTER THE FILE
          OLogManager.instance().debug(this,
              "File '" + fileName + "' is not registered in 'file name - id' map, but exists in file system. Registering it");

          if (fileId == null) {
            ++fileCounter;
            fileId = fileCounter;
          } else
            fileId = -fileId;

          files.put(fileId, fileClassic);
          nameIdMap.put(fileName, fileId);
          writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);
        }
      }

      openFile(fileClassic);

      return composeFileId(id, fileId);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long addFile(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      if (fileId != null && fileId >= 0)
        throw new OStorageException("File with name " + fileName + " already exists in storage " + storageLocal.getName());

      if (fileId == null) {
        ++fileCounter;
        fileId = fileCounter;
      } else
        fileId = -fileId;

      fileClassic = createFile(fileName);

      files.put(fileId, fileClassic);
      nameIdMap.put(fileName, fileId);
      writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

      addFile(fileClassic);

      return composeFileId(id, fileId);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public int internalFileId(long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(int fileId) {
    return composeFileId(id, fileId);
  }

  public void openFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      OFileClassic fileClassic;

      Integer existingFileId = nameIdMap.get(fileName);

      if (existingFileId != null && fileId >= 0) {
        if (existingFileId == extractFileId(fileId))
          fileClassic = files.get(existingFileId);
        else
          throw new OStorageException(
              "File with given name already exists but has different id " + existingFileId + " vs. proposed " + fileId);
      } else {
        throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageLocal.getName());
      }

      openFile(fileClassic);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long addFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      OFileClassic fileClassic;

      Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId)
          throw new OStorageException(
              "File with name '" + fileName + "'' already exists in storage '" + storageLocal.getName() + "'");
        else
          throw new OStorageException(
              "File with given name already exists but has different id " + existingFileId + " vs. proposed " + fileId);
      }

      fileClassic = files.get(intId);

      if (fileClassic != null)
        throw new OStorageException(
            "File with given id exists but has different name " + fileClassic.getName() + " vs. proposed " + fileName);

      if (fileCounter < intId)
        fileCounter = intId;

      fileClassic = createFile(fileName);

      files.put(intId, fileClassic);
      nameIdMap.put(fileName, intId);
      writeNameIdEntry(new NameFileIdEntry(fileName, intId), true);

      addFile(fileClassic);

      return composeFileId(id, intId);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean checkLowDiskSpace() {
    final File storageDir = new File(storagePath);

    final long freeSpace = storageDir.getFreeSpace();
    return freeSpace < freeSpaceLimit;
  }

  public void makeFuzzyCheckpoint() {
    if (writeAheadLog != null) {
      writeAheadLog.flush();
      Future<?> future = commitExecutor.submit(new PeriodicalFuzzyCheckpointTask());
      try {
        future.get();
      } catch (Exception e) {
        throw OException
            .wrapException(new OStorageException("Error during fuzzy checkpoint execution for storage " + storageLocal.getName()),
                e);
      }
    }
  }

  public void lock() throws IOException {
    for (OFileClassic file : files.values()) {
      file.lock();
    }
  }

  public void unlock() throws IOException {
    for (OFileClassic file : files.values()) {
      file.unlock();
    }
  }

  public void openFile(long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      final int intId = extractFileId(fileId);

      final OFileClassic fileClassic = files.get(intId);
      if (fileClassic == null)
        throw new OStorageException("File with id " + fileId + " does not exist.");

      openFile(fileClassic);

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean exists(String fileName) {
    filesLock.acquireReadLock();
    try {
      if (nameIdMap != null) {
        Integer fileId = nameIdMap.get(fileName);

        if (fileId != null && fileId >= 0)
          return true;
      }

      final File file = new File(
          storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath() + File.separator + fileName));
      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public boolean exists(long fileId) {
    filesLock.acquireReadLock();
    try {
      final int intId = extractFileId(fileId);

      final OFileClassic file = files.get(intId);

      if (file == null)
        return false;

      return file.exists();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public Future store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    Future future = null;

    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      final PageKey pageKey = new PageKey(intId, pageIndex);
      Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
      try {
        PageGroup pageGroup = writeCachePages.get(pageKey);
        if (pageGroup == null) {
          pageGroup = new PageGroup(System.currentTimeMillis(), dataPointer);

          writeCachePages.put(pageKey, pageGroup);

          writeCacheSize.increment();

          dataPointer.setWritersListener(this);
          dataPointer.incrementWritersReferrer();
        }

        assert pageGroup.page.equals(dataPointer);

        pageGroup.recencyBit = true;
      } finally {
        lockManager.releaseLock(groupLock);
      }

      if (exclusiveWriteCacheSize.get() > writeCacheMaxSize) {
        cacheOverflowCount.increment();
        future = commitExecutor.submit(new PeriodicFlushTask());
      }

      return future;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public Map<String, Long> files() {
    filesLock.acquireReadLock();
    try {
      final Map<String, Long> result = new HashMap<String, Long>();

      for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getValue()));
        }
      }

      return result;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public OCachePointer[] load(long fileId, long startPageIndex, int pageCount, boolean addNewPages, OModifiableBoolean cacheHit)
      throws IOException {
    final int intId = extractFileId(fileId);
    if (pageCount < 1)
      throw new IllegalArgumentException("Amount of pages to load should be not less than 1 but provided value is " + pageCount);

    filesLock.acquireReadLock();
    try {
      final PageKey[] pageKeys = new PageKey[pageCount];
      for (int i = 0; i < pageCount; i++) {
        pageKeys[i] = new PageKey(intId, startPageIndex + i);
      }

      Lock[] pageLocks = lockManager.acquireSharedLocksInBatch(pageKeys);
      try {
        PageGroup pageGroup = writeCachePages.get(pageKeys[0]);

        if (pageGroup == null) {
          final OCachePointer pagePointers[] = cacheFileContent(fileId, intId, startPageIndex, pageCount, addNewPages, cacheHit);

          if (pagePointers.length == 0)
            return pagePointers;

          for (int n = 0; n < pagePointers.length; n++) {
            pagePointers[n].incrementReadersReferrer();

            if (n > 0) {
              pageGroup = writeCachePages.get(pageKeys[n]);

              assert pageKeys[n].pageIndex == pagePointers[n].getPageIndex();

              if (pageGroup != null) {
                pagePointers[n].decrementReadersReferrer();
                pagePointers[n] = pageGroup.page;
              }
            }
          }

          return pagePointers;
        }

        final OCachePointer pagePointer = pageGroup.page;
        pagePointer.incrementReadersReferrer();

        cacheHit.setValue(true);

        return new OCachePointer[] { pagePointer };
      } finally {
        for (Lock lock : pageLocks) {
          lock.unlock();
        }
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void addOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.increment();
    exclusiveWritePages.add(new PageKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void removeOnlyWriters(final long fileId, final long pageIndex) {
    exclusiveWriteCacheSize.decrement();
    exclusiveWritePages.remove(new PageKey(extractFileId(fileId), pageIndex));
  }

  public void flush(final long fileId) {
    final Future<Void> future = commitExecutor.submit(new FileFlushTask(extractFileId(fileId)));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OInterruptedException("File flush was interrupted");
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  public void flush() {
    for (long fileId : files.keySet())
      flush(fileId);
  }

  public long getFilledUpTo(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      return files.get(intId).getFileSize() / pageSize;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public long getExclusiveWriteCachePagesSize() {
    return exclusiveWriteCacheSize.get();
  }

  public boolean isOpen(final long fileId) {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      OFileClassic fileClassic = files.get(intId);
      if (fileClassic != null)
        return fileClassic.isOpen();

      return false;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public Long isOpen(final String fileName) throws IOException {
    final Long lightResult = isOpenLightCheck(fileName);

    if (lightResult != null)
      return lightResult;

    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      final Integer fileId = nameIdMap.get(fileName);
      if (fileId == null || fileId < 0)
        return null;

      final OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null || !fileClassic.isOpen())
        return null;

      return composeFileId(id, fileId);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  private Long isOpenLightCheck(final String fileName) {
    ConcurrentMap<String, Integer> map = this.nameIdMap;

    if (map != null) {
      final Integer fileId = map.get(fileName);
      if (fileId == null || fileId < 0)
        return null;

      final OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null || !fileClassic.isOpen())
        return null;

      return composeFileId(id, fileId);
    }

    return null;
  }

  public void deleteFile(final long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      final String name = doDeleteFile(intId);

      if (name != null) {
        nameIdMap.put(name, -intId);
        writeNameIdEntry(new NameFileIdEntry(name, -intId), true);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void truncateFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      if (!isOpen(fileId))
        return;

      removeCachedPages(intId);
      files.get(intId).shrink(0);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      if (!files.containsKey(intId))
        return;

      final OFileClassic file = files.get(intId);
      final String osFileName = file.getName();
      if (osFileName.startsWith(oldFileName)) {
        final File newFile = new File(storageLocal.getStoragePath() + File.separator + newFileName + osFileName
            .substring(osFileName.lastIndexOf(oldFileName) + oldFileName.length()));
        boolean renamed = file.renameTo(newFile);
        while (!renamed) {
          renamed = file.renameTo(newFile);
        }
      }

      nameIdMap.remove(oldFileName);
      nameIdMap.put(newFileName, intId);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, intId), true);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long[] close() throws IOException {
    flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OWriteCacheException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw OException.wrapException(new OWriteCacheException("Data flush thread was interrupted"), e);
      }
    }

    filesLock.acquireWriteLock();
    try {

      long[] result = new long[files.size()];
      int counter = 0;
      for (Map.Entry<Integer, OFileClassic> fileEntry : files.entrySet()) {
        OFileClassic fileClassic = fileEntry.getValue();
        if (fileClassic.isOpen())
          fileClassic.close();

        result[counter++] = composeFileId(id, fileEntry.getKey());
      }

      if (nameIdMapHolder != null) {
        nameIdMapHolder.setLength(0);

        for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
          writeNameIdEntry(new NameFileIdEntry(entry.getKey(), entry.getValue()), false);
        }
        nameIdMapHolder.getFD().sync();
        nameIdMapHolder.close();
      }

      return result;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void close(long fileId, boolean flush) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireWriteLock();
    try {
      if (!isOpen(intId))
        return;

      if (flush)
        flush(intId);
      else
        removeCachedPages(intId);

      files.get(intId).close();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;
    final List<OPageDataVerificationError> errors = new ArrayList<OPageDataVerificationError>();

    filesLock.acquireWriteLock();
    try {
      for (Map.Entry<Integer, OFileClassic> entry : files.entrySet()) {
        final int fileId = entry.getKey();
        final OFileClassic fileClassic = entry.getValue();

        boolean fileIsCorrect;
        try {

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Flashing file " + fileClassic.getName() + "... ");

          flush(fileId);

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Start verification of content of " + fileClassic.getName() + "file ...");

          long time = System.currentTimeMillis();

          long filledUpTo = fileClassic.getFileSize();
          fileIsCorrect = true;

          for (long pos = 0; pos < filledUpTo; pos += pageSize) {
            boolean checkSumIncorrect = false;
            boolean magicNumberIncorrect = false;

            byte[] data = new byte[pageSize];

            fileClassic.read(pos, data, data.length);

            long magicNumber = OLongSerializer.INSTANCE.deserializeNative(data, 0);

            if (magicNumber != MAGIC_NUMBER) {
              magicNumberIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage(
                    "Error: Magic number for page " + (pos / pageSize) + " in file " + fileClassic.getName()
                        + " does not much !!!");
              fileIsCorrect = false;
            }

            final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, OLongSerializer.LONG_SIZE);

            final int calculatedCRC32 = calculatePageCrc(data);
            if (storedCRC32 != calculatedCRC32) {
              checkSumIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage(
                    "Error: Checksum for page " + (pos / pageSize) + " in file " + fileClassic.getName() + " is incorrect !!!");
              fileIsCorrect = false;
            }

            if (magicNumberIncorrect || checkSumIncorrect)
              errors.add(
                  new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileClassic.getName()));

            if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
              time = notificationTimeOut;
              commandOutputListener.onMessage((pos / pageSize) + " pages were processed ...");
            }
          }
        } catch (IOException ioe) {
          if (commandOutputListener != null)
            commandOutputListener
                .onMessage("Error: Error during processing of file " + fileClassic.getName() + ". " + ioe.getMessage());

          fileIsCorrect = false;
        }

        if (!fileIsCorrect) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is finished with errors.");
        } else {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file " + fileClassic.getName() + " is successfully finished.");
        }
      }

      return errors.toArray(new OPageDataVerificationError[errors.size()]);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long[] delete() throws IOException {
    long[] result = null;
    filesLock.acquireWriteLock();
    try {
      result = new long[files.size()];

      int counter = 0;
      for (int fileId : files.keySet()) {
        doDeleteFile(fileId);
        result[counter++] = composeFileId(id, fileId);
      }

      if (nameIdMapHolderFile != null) {
        if (nameIdMapHolderFile.exists()) {
          nameIdMapHolder.close();

          if (!nameIdMapHolderFile.delete())
            throw new OStorageException("Cannot delete disk cache file which contains name-id mapping.");
        }

        nameIdMapHolder = null;
        nameIdMapHolderFile = null;
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OWriteCacheException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OInterruptedException("Data flush thread was interrupted");
      }
    }

    return result;
  }

  public String fileNameById(long fileId) {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      final OFileClassic f = files.get(intId);
      return f != null ? f.getName() : null;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public int getId() {
    return id;
  }

  public long getCacheOverflowCount() {
    return cacheOverflowCount.get();
  }

  public long getWriteCacheSize() {
    return writeCacheSize.get();
  }

  public long getExclusiveWriteCacheSize() {
    return exclusiveWriteCacheSize.get();
  }

  private void openFile(final OFileClassic fileClassic) throws IOException {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen())
        fileClassic.open();
    } else {
      throw new OStorageException("File " + fileClassic + " does not exist.");
    }

  }

  private void addFile(final OFileClassic fileClassic) throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create();
      fileClassic.synch();
    } else {
      throw new OStorageException("File '" + fileClassic.getName() + "' already exists.");
    }
  }

  private void initNameIdMapping() throws IOException {
    if (nameIdMapHolder == null) {
      final File storagePath = new File(storageLocal.getStoragePath());
      if (!storagePath.exists())
        if (!storagePath.mkdirs())
          throw new OStorageException("Cannot create directories for the path '" + storagePath + "'");

      nameIdMapHolderFile = new File(storagePath, NAME_ID_MAP);

      nameIdMapHolder = new RandomAccessFile(nameIdMapHolderFile, "rw");
      readNameIdMap();
    }
  }

  private OFileClassic createFile(String fileName) {
    String path = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath() + File.separator + fileName);
    OFileClassic fileClassic = new OFileClassic(path, storageLocal.getMode());
    return fileClassic;
  }

  private void readNameIdMap() throws IOException {
    nameIdMap = new ConcurrentHashMap<String, Integer>();
    long localFileCounter = -1;

    nameIdMapHolder.seek(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntry()) != null) {

      final long absFileId = Math.abs(nameFileIdEntry.fileId);
      if (localFileCounter < absFileId)
        localFileCounter = absFileId;

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
    }

    if (localFileCounter > 0)
      fileCounter = (int) localFileCounter;

    for (Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0 && !files.containsKey(nameIdEntry.getValue())) {
        OFileClassic fileClassic = createFile(nameIdEntry.getKey());

        if (fileClassic.exists())
          files.put(nameIdEntry.getValue(), fileClassic);
        else {
          final Integer fileId = nameIdMap.get(nameIdEntry.getKey());

          if (fileId != null && fileId > 0) {
            nameIdMap.put(nameIdEntry.getKey(), -fileId);
          }
        }
      }
    }
  }

  private NameFileIdEntry readNextNameIdEntry() throws IOException {
    try {
      final int nameSize = nameIdMapHolder.readInt();
      byte[] serializedName = new byte[nameSize];

      nameIdMapHolder.readFully(serializedName);

      final String name = stringSerializer.deserialize(serializedName, 0);
      final int fileId = (int) nameIdMapHolder.readLong();

      return new NameFileIdEntry(name, fileId);
    } catch (EOFException eof) {
      return null;
    }
  }

  private void writeNameIdEntry(NameFileIdEntry nameFileIdEntry, boolean sync) throws IOException {

    nameIdMapHolder.seek(nameIdMapHolder.length());

    final int nameSize = stringSerializer.getObjectSize(nameFileIdEntry.name);
    byte[] serializedRecord = new byte[OIntegerSerializer.INT_SIZE + nameSize + OLongSerializer.LONG_SIZE];
    OIntegerSerializer.INSTANCE.serializeLiteral(nameSize, serializedRecord, 0);
    stringSerializer.serialize(nameFileIdEntry.name, serializedRecord, OIntegerSerializer.INT_SIZE);
    OLongSerializer.INSTANCE.serializeLiteral(nameFileIdEntry.fileId, serializedRecord, OIntegerSerializer.INT_SIZE + nameSize);

    nameIdMapHolder.write(serializedRecord);

    if (sync)
      nameIdMapHolder.getFD().sync();
  }

  private String doDeleteFile(int fileId) throws IOException {
    final int intId = extractFileId(fileId);
    if (isOpen(fileId))
      removeCachedPages(intId);

    final OFileClassic fileClassic = files.remove(fileId);

    String name = null;
    if (fileClassic != null) {
      name = fileClassic.getName();

      if (fileClassic.exists())
        fileClassic.delete();
    }

    return name;
  }

  private void removeCachedPages(int fileId) {
    Future<Void> future = commitExecutor.submit(new RemoveFilePagesTask(fileId));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OInterruptedException("File data removal was interrupted");
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File data removal was abnormally terminated"), e);
    }
  }

  private OCachePointer[] cacheFileContent(final long fileId, final int intId, final long startPageIndex, final int pageCount,
      final boolean addNewPages, OModifiableBoolean cacheHit) throws IOException {

    final OFileClassic fileClassic = files.get(intId);

    if (fileClassic == null)
      throw new IllegalArgumentException("File with id " + intId + " not found in WOW Cache");

    final OLogSequenceNumber lastLsn;
    if (writeAheadLog != null)
      lastLsn = writeAheadLog.getFlushedLsn();
    else
      lastLsn = new OLogSequenceNumber(-1, -1);

    final long firstPageStartPosition = startPageIndex * pageSize;
    final long firstPageEndPosition = firstPageStartPosition + pageSize;

    if (fileClassic.getFileSize() >= firstPageEndPosition) {
      final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
          .getSessionPerformanceStatistic();
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      }

      int pagesRead = 0;

      try {
        if (pageCount == 1) {
          final ByteBuffer buffer = bufferPool.acquireDirect(false);
          fileClassic.read(firstPageStartPosition, buffer);
          buffer.position(0);

          final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, lastLsn, fileId, startPageIndex);
          pagesRead = 1;
          return new OCachePointer[] { dataPointer };
        }

        final long maxPageCount = (fileClassic.getFileSize() - firstPageStartPosition) / pageSize;
        final int realPageCount = Math.min((int) maxPageCount, pageCount);

        final ByteBuffer[] buffers = new ByteBuffer[realPageCount];
        for (int i = 0; i < buffers.length; i++) {
          buffers[i] = bufferPool.acquireDirect(false);
          assert buffers[i].position() == 0;
        }

        final long bytesRead = fileClassic.read(firstPageStartPosition, buffers);
        assert bytesRead % pageSize == 0;

        final int buffersRead = (int) (bytesRead / pageSize);

        final OCachePointer[] dataPointers = new OCachePointer[buffersRead];
        for (int n = 0; n < buffersRead; n++) {
          buffers[n].position(0);
          dataPointers[n] = new OCachePointer(buffers[n], bufferPool, lastLsn, fileId, startPageIndex + n);
        }

        for (int n = buffersRead; n < buffers.length; n++) {
          bufferPool.release(buffers[n]);
        }

        pagesRead = dataPointers.length;
        return dataPointers;
      } finally {
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(pagesRead);
        }

      }
    } else if (addNewPages) {
      final int space = (int) (firstPageEndPosition - fileClassic.getFileSize());

      if (space > 0)
        fileClassic.allocateSpace(space);

      freeSpaceCheckAfterNewPageAdd();

      final ByteBuffer buffer = bufferPool.acquireDirect(true);
      final OCachePointer dataPointer = new OCachePointer(buffer, bufferPool, lastLsn, fileId, startPageIndex);

      cacheHit.setValue(true);
      return new OCachePointer[] { dataPointer };
    } else
      return new OCachePointer[0];
  }

  private void flushPage(final int fileId, final long pageIndex, final ByteBuffer buffer) throws IOException {
    if (writeAheadLog != null) {
      final OLogSequenceNumber lsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
      final OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = new byte[pageSize];
    buffer.position(0);
    buffer.get(content);

    OLongSerializer.INSTANCE.serializeNative(MAGIC_NUMBER, content, 0);

    final int crc32 = calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(fileId);
    fileClassic.write(pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();
  }

  private static final class NameFileIdEntry {
    private final String name;
    private final int    fileId;

    private NameFileIdEntry(String name, int fileId) {
      this.name = name;
      this.fileId = fileId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      NameFileIdEntry that = (NameFileIdEntry) o;

      if (fileId != that.fileId)
        return false;
      if (!name.equals(that.name))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + fileId;
      return result;
    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final int  fileId;
    private final long pageIndex;

    private PageKey(final int fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(final PageKey other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (pageIndex > other.pageIndex)
        return 1;
      if (pageIndex < other.pageIndex)
        return -1;

      return 0;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId)
        return false;
      if (pageIndex != pageKey.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fileId;
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    public PageKey previous() {
      return pageIndex == -1 ? this : new PageKey(fileId, pageIndex - 1);
    }
  }

  private final class PeriodicFlushTask implements Runnable {

    @Override
    public void run() {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      if (statistic != null)
        statistic.startWriteCacheFlushTimer();

      int flushedPages = 0;
      boolean iterateByWritePagesFirst = false;
      try {
        if (writeCachePages.isEmpty()) {
          return;
        }

        int writePagesToFlush = 0;

        final long wcs = exclusiveWriteCacheSize.get();
        final long cs = writeCacheSize.get();

        assert wcs >= 0;
        assert cs >= 0;

        boolean forceFlush = false;

        final double writeCacheThreshold = ((double) wcs) / writeCacheMaxSize;

        if (writeCacheThreshold > 0.3) {
          writePagesToFlush = (int) Math.floor(((writeCacheThreshold - 0.3) / 0.4) * MAX_PAGES_PER_FLUSH);
          iterateByWritePagesFirst = true;

          if (writeCacheThreshold > 0.7)
            forceFlush = true;
        }

        final double cacheThreshold = ((double) cs) / cacheMaxSize;
        if (cacheThreshold > 0.3) {
          final int pagesToFlush = (int) Math.floor(((cacheThreshold - 0.3) / 0.4) * MAX_PAGES_PER_FLUSH);

          writePagesToFlush = Math.max(pagesToFlush, writePagesToFlush);
          if (cacheThreshold > 0.7)
            forceFlush = true;
        }

        writePagesToFlush = Math.max(4, Math.min(MAX_PAGES_PER_FLUSH, writePagesToFlush));

        // Obtain page keys with minimum LSNs and rewind them one position back to include them into tailSet/tailMap view later.
        lastPageKey = findNonExclusivePageKeyWithMinimumLsn().previous();
        lastWritePageKey = findExclusivePageKeyWithMinimumLsn().previous();

        flushedPages = flushRing(writePagesToFlush, flushedPages, false, iterateByWritePagesFirst);
        if (flushedPages < writePagesToFlush) {
          flushedPages = flushRing(writePagesToFlush, flushedPages, false, iterateByWritePagesFirst);
        }

        if (flushedPages < writePagesToFlush && iterateByWritePagesFirst) {
          flushedPages = flushRing(writePagesToFlush, flushedPages, false, false);
        }

        if (flushedPages < writePagesToFlush && forceFlush) {
          flushedPages = flushRing(writePagesToFlush, flushedPages, true, iterateByWritePagesFirst);

          if (flushedPages < writePagesToFlush && iterateByWritePagesFirst) {
            flushedPages = flushRing(writePagesToFlush, flushedPages, true, false);

            if (flushedPages < writePagesToFlush) {
              flushedPages = flushRing(writePagesToFlush, flushedPages, true, false);
            }
          }
        }

      } catch (IOException e) {
        OLogManager.instance().error(this, "Exception during data flush", e);
      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "Exception during data flush", e);
      } finally {
        if (statistic != null)
          statistic.stopWriteCacheFlushTimer(flushedPages);
      }
    }

    private int flushRing(final int writePagesToFlush, int flushedPages, final boolean forceFlush,
        final boolean iterateByWritePagesFirst) throws IOException {

      NavigableMap<PageKey, PageGroup> subMap = null;
      NavigableSet<PageKey> writePagesSubset = null;

      if (iterateByWritePagesFirst) {
        writePagesSubset = exclusiveWritePages.tailSet(lastWritePageKey, false);
      } else {
        subMap = writeCachePages.tailMap(lastPageKey, false);
      }

      flushedPages = iterateBySubRing(subMap, writePagesSubset, writePagesToFlush, flushedPages, forceFlush,
          iterateByWritePagesFirst);

      if (flushedPages < writePagesToFlush) {
        flushedPages = iterateBySubRing(writeCachePages, exclusiveWritePages, writePagesToFlush, flushedPages, forceFlush,
            iterateByWritePagesFirst);
      }

      return flushedPages;
    }

    private PageKey findExclusivePageKeyWithMinimumLsn() {
      PageKey result = lastWritePageKey;

      OLogSequenceNumber minimumLsn = new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
      for (PageKey pageKey : exclusiveWritePages) {
        final Lock lock = lockManager.acquireExclusiveLock(pageKey);
        try {
          final PageGroup pageGroup = writeCachePages.get(pageKey);
          if (pageGroup == null)
            continue;

          final OLogSequenceNumber lsn = pageGroup.page.getLastFlushedLsn();
          if (lsn != null && lsn.compareTo(minimumLsn) < 0) {
            minimumLsn = lsn;
            result = pageKey;
          }
        } finally {
          lockManager.releaseLock(lock);
        }
      }

      return result;
    }

    private PageKey findNonExclusivePageKeyWithMinimumLsn() {
      PageKey result = lastPageKey;

      OLogSequenceNumber minimumLsn = new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
      for (Map.Entry<PageKey, PageGroup> entry : writeCachePages.entrySet()) {
        final PageKey pageKey = entry.getKey();
        final Lock lock = lockManager.acquireExclusiveLock(pageKey);
        try {
          final PageGroup pageGroup = entry.getValue();

          final OLogSequenceNumber lsn = pageGroup.page.getLastFlushedLsn();
          if (lsn != null && lsn.compareTo(minimumLsn) < 0) {
            minimumLsn = lsn;
            result = pageKey;
          }
        } finally {
          lockManager.releaseLock(lock);
        }
      }

      return result;
    }

    private int iterateBySubRing(final NavigableMap<PageKey, PageGroup> subMap, NavigableSet<PageKey> subSet, int writePagesToFlush,
        int flushedWritePages, boolean forceFlush, boolean iterateByWritePagesFirst) throws IOException {
      if (!iterateByWritePagesFirst) {
        return iterateByCacheSubRing(subMap, writePagesToFlush, flushedWritePages, forceFlush);
      } else {
        return iterateByWritePagesSubRing(subSet, writePagesToFlush, flushedWritePages, forceFlush);
      }
    }

    private int iterateByWritePagesSubRing(final NavigableSet<PageKey> subSet, final int writePagesToFlush, int flushedWritePages,
        final boolean forceFlush) throws IOException {
      final Iterator<PageKey> entriesIterator = subSet.iterator();
      final long currentTime = System.currentTimeMillis();
      final long maxSegmentDistance =
          writeAheadLog == null ? -1 : (long) (writeAheadLog.getPreferredSegmentCount() * MAX_LSN_SEGMENT_DISTANCE_FACTOR);

      int flushedRegions = 0;

      long lastPageIndex = -1;
      while (entriesIterator.hasNext()) {
        PageKey entry = entriesIterator.next();
        if (lastPageIndex >= 0) {
          if (entry.pageIndex != lastPageIndex + 1) {
            flushedRegions++;
          }
        }

        if (flushedWritePages > writePagesToFlush && flushedRegions >= 4)
          break;

        Lock groupLock = lockManager.acquireExclusiveLock(entry);
        try {

          PageGroup group = writeCachePages.get(entry);
          if (group == null) {
            entriesIterator.remove();
            continue;
          }

          final OCachePointer pagePointer = group.page;
          final boolean weakLockMode =
              currentTime - group.creationTime < groupTTL && !forceFlush && !tooOldLsn(pagePointer.getLastFlushedLsn(),
                  maxSegmentDistance);

          if (group.recencyBit && weakLockMode) {
            group.recencyBit = false;
            continue;
          } else {
            group.recencyBit = false;

            if (weakLockMode) {
              if (!pagePointer.tryAcquireSharedLock())
                continue;
            } else
              pagePointer.acquireSharedLock();

            try {
              final ByteBuffer buffer = pagePointer.getSharedBuffer();
              flushPage(entry.fileId, entry.pageIndex, buffer);

              final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(buffer);
              pagePointer.setLastFlushedLsn(flushedLSN);
            } finally {
              pagePointer.releaseSharedLock();
            }

            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);

            entriesIterator.remove();
            writeCachePages.remove(entry);
          }
        } finally {
          lockManager.releaseLock(groupLock);
        }

        lastWritePageKey = entry;
        flushedWritePages++;

        lastPageIndex = entry.pageIndex;

        writeCacheSize.decrement();
      }

      return flushedWritePages;
    }

    private int iterateByCacheSubRing(final NavigableMap<PageKey, PageGroup> subMap, final int writePagesToFlush,
        int flushedWritePages, final boolean forceFlush) throws IOException {
      final Iterator<Map.Entry<PageKey, PageGroup>> entriesIterator = subMap.entrySet().iterator();
      final long currentTime = System.currentTimeMillis();
      final long maxSegmentDistance =
          writeAheadLog == null ? -1 : (long) (writeAheadLog.getPreferredSegmentCount() * MAX_LSN_SEGMENT_DISTANCE_FACTOR);

      int flushedRegions = 0;

      long lastPageIndex = -1;
      while (entriesIterator.hasNext()) {
        Map.Entry<PageKey, PageGroup> entry = entriesIterator.next();

        final PageGroup group = entry.getValue();
        final PageKey pageKey = entry.getKey();

        if (lastPageIndex >= 0) {
          if (pageKey.pageIndex != lastPageIndex + 1) {
            flushedRegions++;

            if (flushedWritePages > writePagesToFlush && flushedRegions >= 4)
              break;
          }
        }

        final OCachePointer pagePointer = group.page;
        final boolean weakLockMode =
            currentTime - group.creationTime < groupTTL && !forceFlush && !tooOldLsn(pagePointer.getLastFlushedLsn(),
                maxSegmentDistance);

        if (group.recencyBit && weakLockMode) {
          group.recencyBit = false;
          continue;
        }

        final Lock groupLock = lockManager.acquireExclusiveLock(entry.getKey());
        try {
          if (group.recencyBit && weakLockMode) {
            group.recencyBit = false;
            continue;
          } else {
            group.recencyBit = false;

            if (weakLockMode) {
              if (!pagePointer.tryAcquireSharedLock())
                continue;
            } else
              pagePointer.acquireSharedLock();

            try {
              final ByteBuffer buffer = pagePointer.getSharedBuffer();
              flushPage(pageKey.fileId, pageKey.pageIndex, buffer);

              final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(buffer);
              pagePointer.setLastFlushedLsn(flushedLSN);
            } finally {
              pagePointer.releaseSharedLock();
            }

            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);

            entriesIterator.remove();
          }
        } finally {
          lockManager.releaseLock(groupLock);
        }

        lastPageKey = pageKey;

        flushedWritePages++;
        lastPageIndex = pageKey.pageIndex;

        writeCacheSize.decrement();
      }

      return flushedWritePages;
    }

    private boolean tooOldLsn(OLogSequenceNumber lsn, long maxSegmentDistance) {
      if (lsn == null || maxSegmentDistance == -1)
        return false;

      final OLogSequenceNumber walLsn = writeAheadLog.getFlushedLsn();
      return walLsn != null && Math.abs(walLsn.getSegment() - lsn.getSegment()) > maxSegmentDistance;
    }
  }

  private final class PeriodicalFuzzyCheckpointTask implements Runnable {
    private PeriodicalFuzzyCheckpointTask() {
    }

    @Override
    public void run() {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      if (statistic != null)
        statistic.startFuzzyCheckpointTimer();
      try {
        OLogSequenceNumber minLsn = findMinLsn(writeAheadLog.getFlushedLsn(), writeCachePages);
        OLogManager.instance().debug(this, "Start fuzzy checkpoint flushed LSN is %s", minLsn);
        try {
          writeAheadLog.logFuzzyCheckPointStart(minLsn);
          for (OFileClassic fileClassic : files.values()) {
            fileClassic.synch();
          }
          writeAheadLog.logFuzzyCheckPointEnd();
          writeAheadLog.flush();

          if (minLsn.compareTo(new OLogSequenceNumber(-1, -1)) > 0)
            writeAheadLog.cutTill(minLsn);
        } catch (IOException ioe) {
          OLogManager.instance().error(this, "Error during fuzzy checkpoint", ioe);
        }

        OLogManager.instance().debug(this, "End fuzzy checkpoint");
      } finally {
        if (statistic != null)
          statistic.stopFuzzyCheckpointTimer();
      }
    }

    private OLogSequenceNumber findMinLsn(OLogSequenceNumber minLsn, ConcurrentSkipListMap<PageKey, PageGroup> ring) {
      for (Map.Entry<PageKey, PageGroup> entry : ring.entrySet()) {
        final Lock groupLock = lockManager.acquireExclusiveLock(entry.getKey());
        try {
          PageGroup group = entry.getValue();
          final OCachePointer pagePointer = group.page;
          if (pagePointer.getLastFlushedLsn() != null) {
            if (minLsn.compareTo(pagePointer.getLastFlushedLsn()) > 0) {
              minLsn = pagePointer.getLastFlushedLsn();
            }
          }
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }
      return minLsn;
    }
  }

  private final class FileFlushTask implements Callable<Void> {
    private final int fileId;

    private FileFlushTask(final int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final PageKey firstKey = new PageKey(fileId, 0);
      final PageKey lastKey = new PageKey(fileId, Long.MAX_VALUE);

      flushRing(writeCachePages.subMap(firstKey, true, lastKey, true));

      files.get(fileId).synch();
      return null;
    }

    private void flushRing(final NavigableMap<PageKey, PageGroup> subMap) throws IOException {
      final Iterator<Map.Entry<PageKey, PageGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<PageKey, PageGroup> entry = entryIterator.next();
        final PageGroup pageGroup = entry.getValue();
        final PageKey pageKey = entry.getKey();

        final Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
        try {

          final OCachePointer pagePointer = pageGroup.page;

          if (!pagePointer.tryAcquireSharedLock())
            continue;

          try {
            final ByteBuffer buffer = pagePointer.getSharedBuffer();
            flushPage(pageKey.fileId, pageKey.pageIndex, buffer);

            final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(buffer);
            pagePointer.setLastFlushedLsn(flushedLSN);
          } finally {
            pagePointer.releaseSharedLock();
          }

          pagePointer.decrementWritersReferrer();
          pagePointer.setWritersListener(null);

          writeCacheSize.decrement();
          entryIterator.remove();

        } finally {
          lockManager.releaseLock(groupLock);
        }
      }
    }
  }

  private final class RemoveFilePagesTask implements Callable<Void> {
    private final int fileId;

    private RemoveFilePagesTask(int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final PageKey firstKey = new PageKey(fileId, 0);
      final PageKey lastKey = new PageKey(fileId, Long.MAX_VALUE);

      removeFromRing(writeCachePages.subMap(firstKey, true, lastKey, true));

      return null;
    }

    private void removeFromRing(NavigableMap<PageKey, PageGroup> subMap) {
      Iterator<Map.Entry<PageKey, PageGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<PageKey, PageGroup> entry = entryIterator.next();
        PageGroup pageGroup = entry.getValue();
        PageKey pageKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(pageKey);
        try {
          final OCachePointer pagePointer = pageGroup.page;
          pagePointer.acquireExclusiveLock();
          try {
            pagePointer.decrementWritersReferrer();
            pagePointer.setWritersListener(null);
            writeCacheSize.decrement();
          } finally {
            pagePointer.releaseExclusiveLock();
          }

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }
    }
  }

  private static class FlushThreadFactory implements ThreadFactory {
    private final String storageName;

    private FlushThreadFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setPriority(Thread.MAX_PRIORITY);
      thread.setName("OrientDB Write Cache Flush Task (" + storageName + ")");
      return thread;
    }
  }

  private static class LowSpaceEventsPublisherFactory implements ThreadFactory {
    private final String storageName;

    private LowSpaceEventsPublisherFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB Low Disk Space Publisher (" + storageName + ")");
      return thread;
    }
  }
}
