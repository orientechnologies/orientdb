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

import com.orientechnologies.common.collection.closabledictionary.OClosableEntry;
import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.concur.lock.*;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.thread.OThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OWriteCacheException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

  /**
   * Marks pages which have a checksum stored.
   */
  public static final long MAGIC_NUMBER_WITH_CHECKSUM = 0xFACB03FEL;

  /**
   * Marks pages which have no checksum stored.
   */
  public static final long MAGIC_NUMBER_WITHOUT_CHECKSUM = 0xEF30BCAFL;

  private static final int MAGIC_NUMBER_OFFSET = 0;

  private static final int CHECKSUM_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  private static final double MAX_LSN_SEGMENT_DISTANCE_FACTOR = 0.75;

  private static final int PAGE_OFFSET_TO_CHECKSUM_FROM = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

  private static boolean crc32ArraysWarningLogged = false;

  private final long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024L * 1024L;

  private final int diskSizeCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES.getValueAsInteger();

  private final List<WeakReference<OLowDiskSpaceListener>> listeners             = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();
  /**
   * Listeners which are called once we detect that some of the pages of files are broken.
   */
  private final List<WeakReference<OPageIsBrokenListener>> pageIsBrokenListeners = new CopyOnWriteArrayList<WeakReference<OPageIsBrokenListener>>();

  private final AtomicLong lastDiskSpaceCheck = new AtomicLong(0);
  private final String storagePath;

  private final ConcurrentSkipListMap<PageKey, PageGroup> writeCachePages         = new ConcurrentSkipListMap<PageKey, PageGroup>();
  private final ConcurrentSkipListSet<PageKey>            exclusiveWritePages     = new ConcurrentSkipListSet<PageKey>();
  private final ODistributedCounter                       writeCacheSize          = new ODistributedCounter();
  private final ODistributedCounter                       exclusiveWriteCacheSize = new ODistributedCounter();
  private final ODistributedCounter                       cacheOverflowCount      = new ODistributedCounter();

  private final OBinarySerializer<String>                    stringSerializer;
  private final OClosableLinkedContainer<Long, OFileClassic> files;

  private final boolean        syncOnPageFlush;
  private final int            pageSize;
  private final long           groupTTL;
  private final OWriteAheadLog writeAheadLog;
  private final AtomicLong amountOfNewPagesAdded = new AtomicLong();

  private final OLockManager<PageKey> lockManager = new OPartitionedLockManager<PageKey>();

  private final OLocalPaginatedStorage storageLocal;
  private final OReadersWriterSpinLock filesLock = new OReadersWriterSpinLock();
  private final ScheduledExecutorService commitExecutor;

  /**
   * Executor which is used to call event listeners in  background thread
   */
  private final ExecutorService cacheEventsPublisher;

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

  private volatile OChecksumMode checksumMode;

  private Method crc32UpdateByteBuffer;

  /**
   * Listeners which are called when exception in background data flush thread is happened.
   */
  private final List<WeakReference<OBackgroundExceptionListener>> backgroundExceptionListeners = new CopyOnWriteArrayList<WeakReference<OBackgroundExceptionListener>>();

  public OWOWCache(boolean syncOnPageFlush, int pageSize, OByteBufferPool bufferPool, long groupTTL, OWriteAheadLog writeAheadLog,
      long pageFlushInterval, long writeCacheMaxSize, long cacheMaxSize, OLocalPaginatedStorage storageLocal, boolean checkMinSize,
      OClosableLinkedContainer<Long, OFileClassic> files, int id, OChecksumMode checksumMode) {
    filesLock.acquireWriteLock();
    try {
      this.id = id;
      this.files = files;

      this.syncOnPageFlush = syncOnPageFlush;
      this.pageSize = pageSize;
      this.groupTTL = groupTTL;
      this.writeAheadLog = writeAheadLog;
      this.bufferPool = bufferPool;
      this.checksumMode = checksumMode;

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

      // XXX: We are forced to use Java 6 on 2.2.x branch, java.util.zip.CRC32 on Java 6 supports only arrays. But we use direct
      // ByteBuffers and it's slow to read their content into arrays just to compute the CRC32. Use the reflection, Luke.
      try {
        crc32UpdateByteBuffer = CRC32.class.getMethod("update", ByteBuffer.class);
        crc32UpdateByteBuffer.setAccessible(true);
      } catch (NoSuchMethodException e) {
        logCrc32ArraysWarningAndSwitchToArrays(e);
      }

      commitExecutor = new OScheduledThreadPoolExecutorWithLogging(1, new FlushThreadFactory(storageLocal.getName()));
      cacheEventsPublisher = new OThreadPoolExecutorWithLogging(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new CacheEventsPublisherFactory(storageLocal.getName()));

      MAX_PAGES_PER_FLUSH = (int) (4000 / (1000.0 / pageFlushInterval));

      if (pageFlushInterval > 0)
        commitExecutor.scheduleWithFixedDelay(new PeriodicFlushTask(), pageFlushInterval, pageFlushInterval, TimeUnit.MILLISECONDS);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Loads files already registered in storage. Has to be called before usage of this cache
   */
  public void loadRegisteredFiles() throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  /**
   * Adds listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to trigger
   */
  @Override
  public void addBackgroundExceptionListener(OBackgroundExceptionListener listener) {
    backgroundExceptionListeners.add(new WeakReference<OBackgroundExceptionListener>(listener));
  }

  /**
   * Removes listener which is triggered if exception is cast inside background flush data thread.
   *
   * @param listener Listener to remove
   */
  @Override
  public void removeBackgroundExceptionListener(OBackgroundExceptionListener listener) {
    List<WeakReference<OBackgroundExceptionListener>> itemsToRemove = new ArrayList<WeakReference<OBackgroundExceptionListener>>();

    for (WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener l = ref.get();
      if (l != null && l.equals(listener)) {
        itemsToRemove.add(ref);
      }
    }

    backgroundExceptionListeners.removeAll(itemsToRemove);
  }

  /**
   * Fires event about exception is thrown in data flush thread
   */
  private void fireBackgroundDataProcessingExceptionEvent(Throwable e) {
    for (WeakReference<OBackgroundExceptionListener> ref : backgroundExceptionListeners) {
      final OBackgroundExceptionListener listener = ref.get();
      if (listener != null) {
        listener.onException(e);
      }
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

  @Override
  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    listeners.add(new WeakReference<OLowDiskSpaceListener>(listener));
  }

  @Override
  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    final List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<WeakReference<OLowDiskSpaceListener>>();

    for (WeakReference<OLowDiskSpaceListener> ref : listeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    listeners.removeAll(itemsToRemove);
  }

  /**
   * @inheritDoc
   */
  @Override
  public void addPageIsBrokenListener(OPageIsBrokenListener listener) {
    pageIsBrokenListeners.add(new WeakReference<OPageIsBrokenListener>(listener));
  }

  /**
   * @inheritDoc
   */
  @Override
  public void removePageIsBrokenListener(OPageIsBrokenListener listener) {
    final List<WeakReference<OPageIsBrokenListener>> itemsToRemove = new ArrayList<WeakReference<OPageIsBrokenListener>>();

    for (WeakReference<OPageIsBrokenListener> ref : pageIsBrokenListeners) {
      final OPageIsBrokenListener pageIsBrokenListener = ref.get();

      if (pageIsBrokenListener == null || pageIsBrokenListener.equals(listener))
        itemsToRemove.add(ref);
    }

    pageIsBrokenListeners.removeAll(itemsToRemove);
  }

  private void callPageIsBrokenListeners(final String fileName, final long pageIndex) {
    cacheEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (WeakReference<OPageIsBrokenListener> pageIsBrokenListenerWeakReference : pageIsBrokenListeners) {
          final OPageIsBrokenListener listener = pageIsBrokenListenerWeakReference.get();
          if (listener != null)
            try {
              listener.pageIsBroken(fileName, pageIndex);
            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "Error during notification of page is broken for storage " + storageLocal.getName(), e);
            }
        }
      }
    });
  }

  private void freeSpaceCheckAfterNewPageAdd(int pagesAdded) {
    final long newPagesAdded = amountOfNewPagesAdded.addAndGet(pagesAdded);
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (newPagesAdded - lastSpaceCheck > diskSizeCheckInterval || lastSpaceCheck == 0) {
      final File storageDir = new File(storagePath);

      final long freeSpace = storageDir.getFreeSpace();
      //we work with virtual devices which have "unlimited" amount of free space
      if (freeSpace < 0) {
        return;
      }

      if (freeSpace < freeSpaceLimit)
        callLowSpaceListeners(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));

      lastDiskSpaceCheck.lazySet(newPagesAdded);
    }
  }

  private void callLowSpaceListeners(final OLowDiskSpaceInformation information) {
    cacheEventsPublisher.execute(new Runnable() {
      @Override
      public void run() {
        for (WeakReference<OLowDiskSpaceListener> lowDiskSpaceListenerWeakReference : listeners) {
          final OLowDiskSpaceListener listener = lowDiskSpaceListenerWeakReference.get();
          if (listener != null)
            try {
              listener.lowDiskSpace(information);
            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "Error during notification of low disk space for storage '" + storageLocal.getName() + "'", e);
            }
        }
      }
    });
  }

  private static int calculatePageCrc(byte[] pageData) {
    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, PAGE_OFFSET_TO_CHECKSUM_FROM, pageData.length - PAGE_OFFSET_TO_CHECKSUM_FROM);
    return (int) crc32.getValue();
  }

  @Override
  public long bookFileId(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
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

  public long loadFile(final String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      //check that file is already registered
      if (!(fileId == null || fileId < 0)) {
        final long externalId = composeFileId(id, fileId);
        fileClassic = files.get(externalId);

        if (fileClassic != null)
          return externalId;
        else
          throw new OStorageException("File with given name '" + fileName + "' only partially registered in storage");
      }

      fileClassic = createFileInstance(fileName);
      if (!fileClassic.exists())
        throw new OStorageException("File with name '" + fileName + "' does not exist in storage '" + storageLocal.getName() + "'");
      else {
        // REGISTER THE FILE
        OLogManager.instance().debug(this,
            "File '" + fileName + "' is not registered in 'file name - id' map, but exists in file system. Registering it...");

        if (fileId == null) {
          ++fileCounter;
          fileId = fileCounter;
        } else
          fileId = -fileId;

        long externalId = composeFileId(id, fileId);
        while (files.get(externalId) != null) {
          ++fileCounter;
          fileId = fileCounter;
          externalId = composeFileId(id, fileId);
        }

        openFile(fileClassic);

        files.add(externalId, fileClassic);

        nameIdMap.put(fileName, fileId);
        writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

        return externalId;
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long addFile(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      Integer fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      if (fileId != null && fileId >= 0)
        throw new OStorageException("File with name '" + fileName + "' already exists in storage '" + storageLocal.getName() + "'");

      if (fileId == null) {
        ++fileCounter;
        fileId = fileCounter;
      } else
        fileId = -fileId;

      fileClassic = createFileInstance(fileName);
      createFile(fileClassic);

      final long externalId = composeFileId(id, fileId);
      files.add(externalId, fileClassic);

      nameIdMap.put(fileName, fileId);
      writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

      return externalId;
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public long fileIdByName(String fileName) {
    final Integer intId = nameIdMap.get(fileName);

    if (intId == null || intId < 0)
      return -1;

    return composeFileId(id, intId);
  }

  @Override
  public int internalFileId(long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(int fileId) {
    return composeFileId(id, fileId);
  }

  public long addFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      OFileClassic fileClassic;

      Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId)
          throw new OStorageException(
              "File with name '" + fileName + "' already exists in storage '" + storageLocal.getName() + "'");
        else {
          final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalFileId(existingFileId));

          try {
            if (entry != null) {
              fileClassic = entry.get();

              if (fileClassic.exists()) {
                throw new OStorageException(
                    "File with given name '" + fileName + "' already exists but has different id " + existingFileId
                        + " vs. proposed " + intId);
              }
            }
          } finally {
            files.release(entry);
          }

          deleteFile(fileId);
        }
      }

      fileId = composeFileId(id, intId);
      fileClassic = files.get(fileId);

      if (fileClassic != null) {
        if (!fileClassic.getName().equals(fileName))
          throw new OStorageException(
              "File with given id exists but has different name '" + fileClassic.getName() + "' vs. proposed '" + fileName + "'");
      } else {
        if (fileCounter < intId)
          fileCounter = intId;

        fileClassic = createFileInstance(fileName);
        createFile(fileClassic);

        files.add(fileId, fileClassic);
      }

      nameIdMap.put(fileName, intId);
      writeNameIdEntry(new NameFileIdEntry(fileName, intId), true);

      return fileId;
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean checkLowDiskSpace() {
    final File storageDir = new File(storagePath);

    final long freeSpace = storageDir.getFreeSpace();
    //system has unlimited amount of free space
    if (freeSpace < 0)
      return true;

    return freeSpace < freeSpaceLimit;
  }

  public void makeFuzzyCheckpoint() {
    if (writeAheadLog != null) {
      writeAheadLog.flush();
      Future<?> future = commitExecutor.submit(new PeriodicalFuzzyCheckpointTask());
      try {
        future.get();
      } catch (Exception e) {
        throw OException.wrapException(
            new OStorageException("Error during fuzzy checkpoint execution for storage '" + storageLocal.getName() + "'"), e);
      }
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
      fileId = composeFileId(id, intId);

      final OFileClassic file = files.get(fileId);

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
        if (pageGroup == null)
          pageGroup = doPutInCache(dataPointer, pageKey);

        assert pageGroup.page.equals(dataPointer);

        pageGroup.recencyBit = true;
      } finally {
        lockManager.releaseExclusiveLock(pageKey);
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

  public OCachePointer[] load(long fileId, long startPageIndex, int pageCount, boolean addNewPages, OModifiableBoolean cacheHit,
      boolean verifyChecksums) throws IOException {

    final int intId = extractFileId(fileId);
    if (pageCount < 1)
      throw new IllegalArgumentException("Amount of pages to load should be not less than 1 but provided value is " + pageCount);

    filesLock.acquireReadLock();
    try {
      //first check that requested page is already cached so we do not need to load it from file
      final PageKey startPageKey = new PageKey(intId, startPageIndex);
      final Lock startPageLock = lockManager.acquireSharedLock(startPageKey);

      //check if page already presented in write cache
      final PageGroup startPageGroup = writeCachePages.get(startPageKey);

      //page is not cached load it from file
      if (startPageGroup == null) {
        //load it from file and preload requested pages
        //there is small optimization
        //if we need single page no need to release already locked page
        Lock[] pageLocks;
        PageKey[] pageKeys;

        if (pageCount > 1) {
          startPageLock.unlock();

          pageKeys = new PageKey[pageCount];
          for (int i = 0; i < pageCount; i++) {
            pageKeys[i] = new PageKey(intId, startPageIndex + i);
          }

          pageLocks = lockManager.acquireSharedLocksInBatch(pageKeys);
        } else {
          pageLocks = new Lock[] { startPageLock };
          pageKeys = new PageKey[] { startPageKey };
        }

        OCachePointer pagePointers[];
        try {
          //load requested page and preload requested amount of pages
          pagePointers = loadFileContent(intId, startPageIndex, pageCount, verifyChecksums);

          if (pagePointers != null) {
            assert pagePointers.length > 0;

            for (int n = 0; n < pagePointers.length; n++) {
              pagePointers[n].incrementReadersReferrer();

              if (n > 0) {
                PageGroup pageGroup = writeCachePages.get(pageKeys[n]);

                assert pageKeys[n].pageIndex == pagePointers[n].getPageIndex();

                //if page already exists in cache we should drop already loaded page and load cache page instead
                if (pageGroup != null) {
                  pagePointers[n].decrementReadersReferrer();
                  pagePointers[n] = pageGroup.page;
                  pagePointers[n].incrementReadersReferrer();
                }
              }
            }

            return pagePointers;
          }

        } finally {
          for (Lock pageLock : pageLocks) {
            pageLock.unlock();
          }
        }

        //requested page is out of file range
        //we need to allocate pages on the disk first
        if (!addNewPages)
          return new OCachePointer[0];

        final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
        try {
          final OFileClassic fileClassic = entry.get();

          long startAllocationIndex = fileClassic.getFileSize() / pageSize;
          long stopAllocationIndex = startPageIndex;

          final PageKey[] allocationPageKeys = new PageKey[(int) (stopAllocationIndex - startAllocationIndex + 1)];

          for (long pageIndex = startAllocationIndex; pageIndex <= stopAllocationIndex; pageIndex++) {
            int index = (int) (pageIndex - startAllocationIndex);
            allocationPageKeys[index] = new PageKey(intId, pageIndex);
          }

          //use exclusive locks to prevent to have duplication of pointers
          //when page is loaded from file because space is already allocated
          //but it the same moment another page for the same index is added to the write cache
          Lock[] locks = lockManager.acquireExclusiveLocksInBatch(allocationPageKeys);
          try {
            final long fileSize = fileClassic.getFileSize();
            final long spaceToAllocate = ((stopAllocationIndex + 1) * pageSize - fileSize);

            OCachePointer resultPointer = null;

            if (spaceToAllocate > 0) {
              final OLogSequenceNumber lastLsn =
                  writeAheadLog == null ? new OLogSequenceNumber(-1, -1) : writeAheadLog.getFlushedLsn();

              fileClassic.allocateSpace(spaceToAllocate);
              startAllocationIndex = fileSize / pageSize;

              for (long index = startAllocationIndex; index <= stopAllocationIndex; index++) {
                final ByteBuffer buffer = bufferPool.acquireDirect(true);
                buffer.putLong(MAGIC_NUMBER_OFFSET, MAGIC_NUMBER_WITHOUT_CHECKSUM);

                final OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, lastLsn, fileId, index);

                //item only in write cache till we will not return
                //it to read cache so we increment exclusive size by one
                //otherwise call of write listener inside pointer may set exclusive size to negative value
                exclusiveWriteCacheSize.increment();

                doPutInCache(cachePointer, new PageKey(intId, index));

                if (index == startPageIndex) {
                  resultPointer = cachePointer;
                }
              }

              //we check is it enough space on disk to continue to write data on it
              //otherwise we switch storage in read-only mode
              freeSpaceCheckAfterNewPageAdd((int) (stopAllocationIndex - startAllocationIndex + 1));
            }

            if (resultPointer != null) {
              resultPointer.incrementReadersReferrer();

              cacheHit.setValue(true);

              return new OCachePointer[] { resultPointer };
            }
          } finally {
            for (Lock lock : locks) {
              lock.unlock();
            }
          }
        } finally {
          files.release(entry);
        }

        //this is case when we allocated space but requested page was outside of allocated space
        //in such case we read it again
        return load(fileId, startPageIndex, pageCount, true, cacheHit, verifyChecksums);

      } else {
        startPageGroup.page.incrementReadersReferrer();
        startPageLock.unlock();

        cacheHit.setValue(true);

        return new OCachePointer[] { startPageGroup.page };
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Load was interrupted"), e);
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
      throw OException.wrapException(new OInterruptedException("File flush was interrupted"), e);
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File flush was abnormally terminated"), e);
    }
  }

  public void flush() {
    int counter = 0;
    for (int intId : nameIdMap.values()) {
      if (intId < 0)
        continue;

      final long externalId = composeFileId(id, intId);
      flush(externalId);
      counter++;
    }

  }

  public long getFilledUpTo(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        return entry.get().getFileSize() / pageSize;
      } finally {
        files.release(entry);
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public long getExclusiveWriteCachePagesSize() {
    return exclusiveWriteCacheSize.get();
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
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      removeCachedPages(intId);
      OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        entry.get().shrink(0);
      } finally {
        files.release(entry);
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      if (entry == null)
        return;

      try {
        OFileClassic file = entry.get();
        final String osFileName = file.getName();
        if (osFileName.startsWith(oldFileName)) {
          final File newFile = new File(storageLocal.getStoragePath() + File.separator + newFileName + osFileName
              .substring(osFileName.lastIndexOf(oldFileName) + oldFileName.length()));
          boolean renamed = file.renameTo(newFile);
          while (!renamed) {
            renamed = file.renameTo(newFile);
          }
        }
      } finally {
        files.release(entry);
      }

      nameIdMap.remove(oldFileName);
      nameIdMap.put(newFileName, intId);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, intId), true);
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
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
        OLogManager.instance().error(this, "Data flush thread was interrupted", e);

        Thread.currentThread().interrupt();
        throw OException.wrapException(new OWriteCacheException("Data flush thread was interrupted"), e);
      }
    }

    final List<Long> result = new ArrayList<Long>();

    filesLock.acquireWriteLock();
    try {
      final Collection<Integer> intIds = nameIdMap.values();

      for (Integer intId : intIds) {
        if (intId < 0)
          continue;

        final long fileId = composeFileId(id, intId);
        //we remove files because when we reopen storage we will reload them
        final OFileClassic fileClassic = files.remove(fileId);
        fileClassic.close();

        result.add(fileId);
      }

      if (nameIdMapHolder != null) {
        nameIdMapHolder.setLength(0);

        for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
          writeNameIdEntry(new NameFileIdEntry(entry.getKey(), entry.getValue()), false);
        }
        nameIdMapHolder.getFD().sync();
        nameIdMapHolder.close();
      }

      nameIdMap.clear();

      final long[] ds = new long[result.size()];
      int counter = 0;
      for (long id : result) {
        ds[counter] = id;
        counter++;
      }

      return ds;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void close(long fileId, boolean flush) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireWriteLock();
    try {
      if (flush)
        flush(intId);
      else
        removeCachedPages(intId);

      if (!files.close(fileId))
        throw new OStorageException("Can not close file with id " + internalFileId(fileId) + " because it is still in use");
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  @Override
  public String restoreFileById(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    filesLock.acquireWriteLock();
    try {
      for (Map.Entry<String, Integer> entry : nameIdMap.entrySet()) {
        if (entry.getValue() == -intId) {
          addFile(entry.getKey(), fileId);
          return entry.getKey();
        }
      }
    } finally {
      filesLock.releaseWriteLock();
    }

    return null;
  }

  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;
    final List<OPageDataVerificationError> errors = new ArrayList<OPageDataVerificationError>();

    filesLock.acquireWriteLock();
    try {
      for (Integer intId : nameIdMap.values()) {
        if (intId < 0)
          continue;

        boolean fileIsCorrect;
        final long externalId = composeFileId(id, intId);
        final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
        final OFileClassic fileClassic = entry.get();
        try {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Flushing file '" + fileClassic.getName() + "'...\n");

          flush(intId);

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Start verification of content of '" + fileClassic.getName() + "' file...\n");

          long time = System.currentTimeMillis();

          long filledUpTo = fileClassic.getFileSize();
          fileIsCorrect = true;

          for (long pos = 0; pos < filledUpTo; pos += pageSize) {
            boolean checkSumIncorrect = false;
            boolean magicNumberIncorrect = false;

            byte[] data = new byte[pageSize];

            fileClassic.read(pos, data, data.length);

            long magicNumber = OLongSerializer.INSTANCE.deserializeNative(data, MAGIC_NUMBER_OFFSET);

            if (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM && magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
              magicNumberIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage(
                    "Error: Magic number for page " + (pos / pageSize) + " in file '" + fileClassic.getName()
                        + "' does not match. It could be corrupted\n");
              fileIsCorrect = false;
            }

            if (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
              final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, CHECKSUM_OFFSET);

              final int calculatedCRC32 = calculatePageCrc(data);
              if (storedCRC32 != calculatedCRC32) {
                checkSumIncorrect = true;
                if (commandOutputListener != null)
                  commandOutputListener.onMessage(
                      "Error: Checksum for page " + (pos / pageSize) + " in file '" + fileClassic.getName()
                          + "' is incorrect. It could be corrupted\n");
                fileIsCorrect = false;
              }
            }

            if (magicNumberIncorrect || checkSumIncorrect)
              errors.add(
                  new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileClassic.getName()));

            if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
              time = notificationTimeOut;
              commandOutputListener.onMessage((pos / pageSize) + " pages were processed...\n");
            }
          }
        } catch (IOException ioe) {
          if (commandOutputListener != null)
            commandOutputListener
                .onMessage("Error: Error during processing of file '" + fileClassic.getName() + "': " + ioe.getMessage() + "\n");

          fileIsCorrect = false;
        } finally {
          files.release(entry);
        }

        if (!fileIsCorrect) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file '" + fileClassic.getName() + "' is finished with errors.\n");
        } else {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file '" + fileClassic.getName() + "' is successfully finished.\n");
        }
      }

      return errors.toArray(new OPageDataVerificationError[errors.size()]);
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Thread was interrupted"), e);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long[] delete() throws IOException {
    final List<Long> result = new ArrayList<Long>();

    filesLock.acquireWriteLock();
    try {
      for (int intId : nameIdMap.values()) {
        if (intId < 0)
          continue;

        final long externalId = composeFileId(id, intId);
        doDeleteFile(externalId);
        result.add(externalId);
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
        OLogManager.instance().error(this, "Data flush thread was interrupted", e);

        throw OException.wrapException(new OInterruptedException("Data flush thread was interrupted"), e);
      }
    }

    final long[] ids = new long[result.size()];
    int counter = 0;
    for (long id : result) {
      ids[counter] = id;
      counter++;
    }

    return ids;
  }

  public String fileNameById(long fileId) {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

    filesLock.acquireReadLock();
    try {

      final OFileClassic f = files.get(fileId);
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

  private void createFile(final OFileClassic fileClassic) throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create();
      fileClassic.synch();
    } else {
      throw new OStorageException("File '" + fileClassic.getName() + "' already exists.");
    }
  }

  private void initNameIdMapping() throws IOException, InterruptedException {
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

  private OFileClassic createFileInstance(String fileName) throws InterruptedException {
    final String path = storageLocal.getVariableParser()
        .resolveVariables(storageLocal.getStoragePath() + File.separator + fileName);
    return new OFileClassic(path, storageLocal.getMode());
  }

  private void readNameIdMap() throws IOException, InterruptedException {
    //older versions of ODB incorrectly logged file deletions
    //some deleted files have the same id
    //because we reuse ids of removed files when we re-create them
    //we need to fix this situation
    final Map<Integer, Set<String>> filesWithfNegativeIds = new HashMap<Integer, Set<String>>();

    nameIdMap = new ConcurrentHashMap<String, Integer>();
    long localFileCounter = -1;

    nameIdMapHolder.seek(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntry()) != null) {

      final long absFileId = Math.abs(nameFileIdEntry.fileId);
      if (localFileCounter < absFileId)
        localFileCounter = absFileId;

      final Integer existingId = nameIdMap.get(nameFileIdEntry.name);

      if (existingId != null && existingId < 0) {
        final Set<String> files = filesWithfNegativeIds.get(existingId);

        if (files != null) {
          files.remove(nameFileIdEntry.name);

          if (files.isEmpty()) {
            filesWithfNegativeIds.remove(existingId);
          }
        }
      }

      if (nameFileIdEntry.fileId < 0) {
        Set<String> files = filesWithfNegativeIds.get(nameFileIdEntry.fileId);

        if (files == null) {
          files = new HashSet<String>();
          files.add(nameFileIdEntry.name);
          filesWithfNegativeIds.put(nameFileIdEntry.fileId, files);
        } else {
          files.add(nameFileIdEntry.name);
        }
      }

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
    }

    if (localFileCounter > 0 && fileCounter < localFileCounter)
      fileCounter = (int) localFileCounter;

    for (Map.Entry<String, Integer> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0) {
        final long externalId = composeFileId(id, nameIdEntry.getValue());

        if (files.get(externalId) == null) {
          OFileClassic fileClassic = createFileInstance(nameIdEntry.getKey());

          if (fileClassic.exists()) {
            fileClassic.open();
            files.add(externalId, fileClassic);
          } else {
            final Integer fileId = nameIdMap.get(nameIdEntry.getKey());

            if (fileId != null && fileId > 0) {
              nameIdMap.put(nameIdEntry.getKey(), -fileId);
            }
          }
        }
      }
    }

    final Set<String> fixedFiles = new HashSet<String>();

    for (Map.Entry<Integer, Set<String>> entry : filesWithfNegativeIds.entrySet()) {
      final Set<String> files = entry.getValue();

      if (files.size() > 1) {
        for (String fileName : files) {
          fileCounter++;
          final int nextId = -fileCounter;
          nameIdMap.put(fileName, nextId);

          fixedFiles.add(fileName);
        }
      }
    }

    if (!fixedFiles.isEmpty())
      OLogManager.instance().warn(this, "Removed files " + fixedFiles + " had duplicated ids. Problem is fixed automatically.");
  }

  private NameFileIdEntry readNextNameIdEntry() throws IOException {
    try {
      final int nameSize = nameIdMapHolder.readInt();
      byte[] serializedName = new byte[nameSize];

      nameIdMapHolder.readFully(serializedName);

      final String name = stringSerializer.deserialize(serializedName, 0);
      final int fileId = (int) nameIdMapHolder.readLong();

      return new NameFileIdEntry(name, fileId);
    } catch (EOFException ignore) {
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

  private String doDeleteFile(long fileId) throws IOException {
    final int intId = extractFileId(fileId);
    fileId = composeFileId(id, intId);

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
    //cache already closed or deleted
    if (commitExecutor.isShutdown())
      return;

    Future<Void> future = commitExecutor.submit(new RemoveFilePagesTask(fileId));
    try {
      future.get();
    } catch (InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("File data removal was interrupted"), e);
    } catch (Exception e) {
      throw OException.wrapException(new OWriteCacheException("File data removal was abnormally terminated"), e);
    }
  }

  private OCachePointer[] loadFileContent(final int intId, final long startPageIndex, final int pageCount, boolean verifyChecksums)
      throws IOException {
    final long fileId = composeFileId(id, intId);

    try {
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
      try {
        final OFileClassic fileClassic = entry.get();
        if (fileClassic == null)
          throw new IllegalArgumentException("File with id " + intId + " not found in WOW Cache");

        final long firstPageStartPosition = startPageIndex * pageSize;
        final long firstPageEndPosition = firstPageStartPosition + pageSize;

        if (fileClassic.getFileSize() >= firstPageEndPosition) {
          final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
              .getSessionPerformanceStatistic();
          if (sessionStoragePerformanceStatistic != null) {
            sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
          }

          int pagesRead = 0;

          final OLogSequenceNumber lastLsn = writeAheadLog == null ? new OLogSequenceNumber(-1, -1) : writeAheadLog.getFlushedLsn();

          try {
            if (pageCount == 1) {
              final ByteBuffer buffer = bufferPool.acquireDirect(false);
              assert buffer.position() == 0;

              fileClassic.read(firstPageStartPosition, buffer, false);

              if (verifyChecksums && (checksumMode == OChecksumMode.StoreAndVerify || checksumMode == OChecksumMode.StoreAndThrow
                  || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode))
                verifyChecksum(buffer, fileId, startPageIndex, null);

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

            fileClassic.read(firstPageStartPosition, buffers, false);

            if (verifyChecksums && (checksumMode == OChecksumMode.StoreAndVerify || checksumMode == OChecksumMode.StoreAndThrow
                || checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode))
              for (int i = 0; i < buffers.length; ++i)
                verifyChecksum(buffers[i], fileId, startPageIndex + i, buffers);

            final OCachePointer[] dataPointers = new OCachePointer[buffers.length];
            for (int n = 0; n < buffers.length; n++) {
              buffers[n].position(0);
              dataPointers[n] = new OCachePointer(buffers[n], bufferPool, lastLsn, fileId, startPageIndex + n);
            }

            pagesRead = dataPointers.length;
            return dataPointers;
          } finally {
            if (sessionStoragePerformanceStatistic != null) {
              sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(pagesRead);
            }
          }
        } else
          return null;
      } finally {
        files.release(entry);
      }
    } catch (InterruptedException e) {
      throw OException.wrapException(new OStorageException("Data load was interrupted"), e);
    }
  }

  private PageGroup doPutInCache(OCachePointer dataPointer, PageKey pageKey) {
    final PageGroup pageGroup = new PageGroup(System.currentTimeMillis(), dataPointer);
    writeCachePages.put(pageKey, pageGroup);

    writeCacheSize.increment();

    dataPointer.setWritersListener(this);
    dataPointer.incrementWritersReferrer();

    return pageGroup;
  }

  private void verifyChecksum(ByteBuffer buffer, long fileId, long pageIndex, ByteBuffer[] buffersToRelease) {
    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.position(MAGIC_NUMBER_OFFSET);
    final long magicNumber = OLongSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    if (magicNumber != MAGIC_NUMBER_WITH_CHECKSUM) {
      if (magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM) {
        final String message = "Magic number verification failed for page '" + pageIndex + "' of '" + fileNameById(fileId) + "'.";
        OLogManager.instance().error(this, "%s", null, message);
        if (checksumMode == OChecksumMode.StoreAndThrow) {

          if (buffersToRelease == null)
            bufferPool.release(buffer);
          else
            for (ByteBuffer bufferToRelease : buffersToRelease)
              bufferPool.release(bufferToRelease);

          throw new OStorageException(message);
        } else if (checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode) {
          callPageIsBrokenListeners(fileNameById(fileId), pageIndex);
        }
      }

      return;
    }

    buffer.position(CHECKSUM_OFFSET);
    final int storedChecksum = OIntegerSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    final CRC32 crc32 = new CRC32();
    buffer.position(PAGE_OFFSET_TO_CHECKSUM_FROM);

    boolean fallbackToArray = true;

    if (crc32UpdateByteBuffer != null) {
      try {
        crc32UpdateByteBuffer.invoke(crc32, buffer);
        fallbackToArray = false;
      } catch (IllegalAccessException e) {
        logCrc32ArraysWarningAndSwitchToArrays(e);
      } catch (InvocationTargetException e) {
        logCrc32ArraysWarningAndSwitchToArrays(e);
      }
    }

    if (fallbackToArray) {
      final int dataLength = buffer.limit() - PAGE_OFFSET_TO_CHECKSUM_FROM;
      final byte[] data = new byte[dataLength];
      buffer.get(data, 0, dataLength);
      crc32.update(data);
    }

    final int computedChecksum = (int) crc32.getValue();

    if (computedChecksum != storedChecksum) {
      final String message = "Checksum verification failed for page '" + pageIndex + "' of '" + fileNameById(fileId) + "'.";
      OLogManager.instance().error(this, "%s", null, message);
      if (checksumMode == OChecksumMode.StoreAndThrow) {

        if (buffersToRelease == null)
          bufferPool.release(buffer);
        else
          for (ByteBuffer bufferToRelease : buffersToRelease)
            bufferPool.release(bufferToRelease);

        throw new OStorageException(message);
      } else if (checksumMode == OChecksumMode.StoreAndSwitchReadOnlyMode) {
        callPageIsBrokenListeners(fileNameById(fileId), pageIndex);
      }

    }
  }

  private void flushPage(final int fileId, final long pageIndex, final ByteBuffer buffer) throws IOException, InterruptedException {
    if (writeAheadLog != null) {
      final OLogSequenceNumber lsn = ODurablePage.getLogSequenceNumberFromPage(buffer);
      final OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLsn();

      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = new byte[pageSize];
    buffer.position(0);
    buffer.get(content);

    OLongSerializer.INSTANCE
        .serializeNative(checksumMode == OChecksumMode.Off ? MAGIC_NUMBER_WITHOUT_CHECKSUM : MAGIC_NUMBER_WITH_CHECKSUM, content,
            MAGIC_NUMBER_OFFSET);

    if (checksumMode != OChecksumMode.Off) {
      final int crc32 = calculatePageCrc(content);
      OIntegerSerializer.INSTANCE.serializeNative(crc32, content, CHECKSUM_OFFSET);
    }

    final long externalId = composeFileId(id, fileId);
    final OClosableEntry<Long, OFileClassic> entry = files.acquire(externalId);
    try {
      final OFileClassic fileClassic = entry.get();
      fileClassic.write(pageIndex * pageSize, content);

      if (syncOnPageFlush)
        fileClassic.synch();
    } finally {
      files.release(entry);
    }
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
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Exception during data flush", e);
        OWOWCache.this.fireBackgroundDataProcessingExceptionEvent(e);
      } finally {
        if (statistic != null)
          statistic.stopWriteCacheFlushTimer(flushedPages);
      }
    }

    private int flushRing(final int writePagesToFlush, int flushedPages, final boolean forceFlush,
        final boolean iterateByWritePagesFirst) throws IOException, InterruptedException {

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
          lockManager.releaseExclusiveLock(pageKey);
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
          lockManager.releaseExclusiveLock(pageKey);
        }
      }

      return result;
    }

    private int iterateBySubRing(final NavigableMap<PageKey, PageGroup> subMap, NavigableSet<PageKey> subSet, int writePagesToFlush,
        int flushedWritePages, boolean forceFlush, boolean iterateByWritePagesFirst) throws IOException, InterruptedException {
      if (!iterateByWritePagesFirst) {
        return iterateByCacheSubRing(subMap, writePagesToFlush, flushedWritePages, forceFlush);
      } else {
        return iterateByWritePagesSubRing(subSet, writePagesToFlush, flushedWritePages, forceFlush);
      }
    }

    private int iterateByWritePagesSubRing(final NavigableSet<PageKey> subSet, final int writePagesToFlush, int flushedWritePages,
        final boolean forceFlush) throws IOException, InterruptedException {
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

            if (!pagePointer.tryAcquireSharedLock())
              continue;

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
          lockManager.releaseExclusiveLock(entry);
        }

        lastWritePageKey = entry;
        flushedWritePages++;

        lastPageIndex = entry.pageIndex;

        writeCacheSize.decrement();
      }

      return flushedWritePages;
    }

    private int iterateByCacheSubRing(final NavigableMap<PageKey, PageGroup> subMap, final int writePagesToFlush,
        int flushedWritePages, final boolean forceFlush) throws IOException, InterruptedException {
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

            entriesIterator.remove();
          }
        } finally {
          lockManager.releaseExclusiveLock(entry.getKey());
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
        if (minLsn == null)
          return;

        OLogManager.instance().debug(this, "Start fuzzy checkpoint flushed LSN is %s", minLsn);
        writeAheadLog.logFuzzyCheckPointStart(minLsn);

        for (Integer intId : nameIdMap.values()) {
          if (intId < 0)
            continue;

          final long fileId = composeFileId(id, intId);
          final OClosableEntry<Long, OFileClassic> entry = files.acquire(fileId);
          if (entry == null)
            continue;

          try {
            final OFileClassic fileClassic = entry.get();
            fileClassic.synch();
          } finally {
            files.release(entry);
          }
        }

        writeAheadLog.logFuzzyCheckPointEnd();
        writeAheadLog.flush();

        if (minLsn.compareTo(new OLogSequenceNumber(-1, -1)) > 0)
          writeAheadLog.cutTill(minLsn);

        OLogManager.instance().debug(this, "End fuzzy checkpoint");
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during fuzzy checkpoint", e);
        fireBackgroundDataProcessingExceptionEvent(e);
      } finally {
        if (statistic != null)
          statistic.stopFuzzyCheckpointTimer();
      }
    }

    private OLogSequenceNumber findMinLsn(OLogSequenceNumber minLsn, ConcurrentSkipListMap<PageKey, PageGroup> ring) {
      if (minLsn == null)
        return null;

      for (Map.Entry<PageKey, PageGroup> entry : ring.entrySet()) {
        lockManager.acquireExclusiveLock(entry.getKey());
        try {
          PageGroup group = entry.getValue();
          final OCachePointer pagePointer = group.page;
          if (pagePointer.getLastFlushedLsn() != null) {
            if (minLsn.compareTo(pagePointer.getLastFlushedLsn()) > 0) {
              minLsn = pagePointer.getLastFlushedLsn();
            }
          }
        } finally {
          lockManager.releaseExclusiveLock(entry.getKey());
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

      final long finalId = composeFileId(id, fileId);
      final OClosableEntry<Long, OFileClassic> entry = files.acquire(finalId);
      try {
        entry.get().synch();
      } finally {
        files.release(entry);
      }

      return null;
    }

    private void flushRing(final NavigableMap<PageKey, PageGroup> subMap) throws IOException, InterruptedException {
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
          lockManager.releaseExclusiveLock(pageKey);
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
          lockManager.releaseExclusiveLock(pageKey);
        }
      }
    }
  }

  public void setChecksumMode(OChecksumMode checksumMode) { // for testing purposes only
    this.checksumMode = checksumMode;
  }

  private void logCrc32ArraysWarningAndSwitchToArrays(Exception e) {
    crc32UpdateByteBuffer = null;

    if (!crc32ArraysWarningLogged) {
      crc32ArraysWarningLogged = true;
      OLogManager.instance().warn(this, "Unable to use java.util.zip.CRC32 on byte buffers, switching to arrays. Using arrays "
              + "instead of byte buffers may produce noticeable performance hit. Consider upgrading to Java 8 or newer. Cause: %s",
          e.toString());
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
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    }
  }

  private static class CacheEventsPublisherFactory implements ThreadFactory {
    private final String storageName;

    private CacheEventsPublisherFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB Write Cache Event Publisher  (" + storageName + ")");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());

      return thread;
    }
  }
}
