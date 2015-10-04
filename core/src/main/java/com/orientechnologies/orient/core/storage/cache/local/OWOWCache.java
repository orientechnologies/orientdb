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
import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
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

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OWOWCache extends OAbstractWriteCache implements OWriteCache, OCachePointer.WritersListener, OWOWCacheMXBean {
  // we add 8 bytes before and after cache pages to prevent word tearing in mt case.

  private final int                                        MAX_PAGES_PER_FLUSH;

  public static final int                                  PAGE_PADDING             = 8;

  public static final String                               NAME_ID_MAP_EXTENSION    = ".cm";

  private static final String                              NAME_ID_MAP              = "name_id_map" + NAME_ID_MAP_EXTENSION;

  public static final int                                  MIN_CACHE_SIZE           = 16;

  public static final long                                 MAGIC_NUMBER             = 0xFACB03FEL;

  private final long                                       freeSpaceLimit           = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT
                                                                                        .getValueAsLong() * 1024L * 1024L;

  private final long                                       diskSizeCheckInterval    = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL
                                                                                        .getValueAsInteger() * 1000;
  private final List<WeakReference<OLowDiskSpaceListener>> listeners                = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();

  private final AtomicLong                                 lastDiskSpaceCheck       = new AtomicLong(System.currentTimeMillis());
  private final String                                     storagePath;

  private final ConcurrentSkipListMap<PagedKey, PageGroup> writeCachePages          = new ConcurrentSkipListMap<PagedKey, PageGroup>();
  private final ConcurrentSkipListSet<PagedKey>            exclusiveWritePages      = new ConcurrentSkipListSet<PagedKey>();

  private final OBinarySerializer<String>                  stringSerializer;
  private final Map<Integer, OFileClassic>                 files;
  private final boolean                                    syncOnPageFlush;
  private final int                                        pageSize;
  private final long                                       groupTTL;
  private final OWriteAheadLog                             writeAheadLog;

  private final ODistributedCounter                        writeCacheSize           = new ODistributedCounter();
  private final ODistributedCounter                        exclusiveWriteCacheSize  = new ODistributedCounter();

  private final ONewLockManager<PagedKey>                  lockManager              = new ONewLockManager<PagedKey>();
  private final OLocalPaginatedStorage                     storageLocal;
  private final OReadersWriterSpinLock                     filesLock                = new OReadersWriterSpinLock();
  private final ScheduledExecutorService                   commitExecutor;

  private final ExecutorService                            lowSpaceEventsPublisher;

  private Map<String, Integer>                             nameIdMap;
  private RandomAccessFile                                 nameIdMapHolder;
  private final int                                        writeCacheMaxSize;
  private final int                                        cacheMaxSize;

  private int                                              fileCounter              = 1;

  private PagedKey                                         lastPageKey              = new PagedKey(0, -1);
  private PagedKey                                         lastWritePageKey         = new PagedKey(0, -1);

  private File                                             nameIdMapHolderFile;

  private final ODistributedCounter                        allocatedSpace           = new ODistributedCounter();
  private final int                                        id;

  private final AtomicReference<Date>                      lastFuzzyCheckpointDate  = new AtomicReference<Date>();
  private final AtomicLong                                 lastAmountOfFlushedPages = new AtomicLong();
  private final AtomicLong                                 durationOfLastFlush      = new AtomicLong();

  private final AtomicBoolean                              mbeanIsRegistered        = new AtomicBoolean();
  public static final String                               MBEAN_NAME               = "com.orientechnologies.orient.core.storage.cache.local:type=OWOWCacheMXBean";

  public OWOWCache(boolean syncOnPageFlush, int pageSize, long groupTTL, OWriteAheadLog writeAheadLog, long pageFlushInterval,
      long writeCacheMaxSize, long cacheMaxSize, OLocalPaginatedStorage storageLocal, boolean checkMinSize, int id) {
    filesLock.acquireWriteLock();
    try {
      this.id = id;
      this.files = new ConcurrentHashMap<Integer, OFileClassic>();

      this.syncOnPageFlush = syncOnPageFlush;
      this.pageSize = pageSize;
      this.groupTTL = groupTTL;
      this.writeAheadLog = writeAheadLog;

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

  private int normalizeMemory(long maxSize, int pageSize) {
    long tmpMaxSize = maxSize / (pageSize + 2 * OWOWCache.PAGE_PADDING);
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
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

  private void addAllocatedSpace(final long diff) {
    if (diff == 0)
      return;

    allocatedSpace.add(diff);

    final long ts = System.currentTimeMillis();
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (ts - lastSpaceCheck > diskSizeCheckInterval) {
      final File storageDir = new File(storagePath);

      final long freeSpace = storageDir.getFreeSpace();
      final long effectiveFreeSpace = freeSpace - allocatedSpace.get();

      if (effectiveFreeSpace < freeSpaceLimit)
        callLowSpaceListeners(new OLowDiskSpaceInformation(effectiveFreeSpace, freeSpaceLimit));

      lastDiskSpaceCheck.lazySet(ts);
    }
  }

  private void callLowSpaceListeners(final OLowDiskSpaceInformation information) {
    lowSpaceEventsPublisher.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        for (WeakReference<OLowDiskSpaceListener> lowDiskSpaceListenerWeakReference : listeners) {
          final OLowDiskSpaceListener listener = lowDiskSpaceListenerWeakReference.get();
          if (listener != null)
            try {
              listener.lowDiskSpace(information);
            } catch (Exception e) {
              OLogManager.instance().error(this,
                  "Error during notification of low disk space for storage " + storageLocal.getName(), e);
            }
        }

        return null;
      }
    });
  }

  private static int calculatePageCrc(final byte[] pageData) {
    int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  public long bookFileId(final String fileName) throws IOException {
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
          throw new OStorageException("File with name '" + fileName + "' does not exist in storage '" + storageLocal.getName()
              + "'");
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
        throw new OStorageException("File with name '" + fileName + "' already exists in storage '" + storageLocal.getName() + "'");

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
          throw new OStorageException("File with given name already exists but has different id " + existingFileId
              + " vs. proposed " + fileId);
      } else {
        throw new OStorageException("File with name '" + fileName + "' does not exist in storage '" + storageLocal.getName() + "'");
      }

      openFile(fileClassic);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void addFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      OFileClassic fileClassic;

      Integer existingFileId = nameIdMap.get(fileName);

      final int intId = extractFileId(fileId);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == intId)
          throw new OStorageException("File with name '" + fileName + "'' already exists in storage '" + storageLocal.getName()
              + "'");
        else
          throw new OStorageException("File with given name already exists but has different id " + existingFileId
              + " vs. proposed " + fileId);
      }

      fileClassic = files.get(intId);

      if (fileClassic != null)
        throw new OStorageException("File with given id exists but has different name '" + fileClassic.getName()
            + "' vs. proposed " + fileName);

      if (fileCounter < intId)
        fileCounter = intId;

      fileClassic = createFile(fileName);

      files.put(intId, fileClassic);
      nameIdMap.put(fileName, intId);
      writeNameIdEntry(new NameFileIdEntry(fileName, intId), true);

      addFile(fileClassic);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean checkLowDiskSpace() {
    final File storageDir = new File(storagePath);

    long freeSpace = storageDir.getFreeSpace();
    long effectiveFreeSpace = freeSpace - allocatedSpace.get();

    return effectiveFreeSpace < freeSpaceLimit;
  }

  public void makeFuzzyCheckpoint() {
    if (writeAheadLog != null) {
      writeAheadLog.flush();
      Future<?> future = commitExecutor.submit(new PeriodicalFuzzyCheckpointTask());
      try {
        future.get();
      } catch (Exception e) {
        throw new OStorageException("Error during fuzzy checkpoint execution for storage '" + storageLocal.getName() + "'", e);
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

      final File file = new File(storageLocal.getVariableParser().resolveVariables(
          storageLocal.getStoragePath() + File.separator + fileName));
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
      final PagedKey pagedKey = new PagedKey(intId, pageIndex);
      Lock groupLock = lockManager.acquireExclusiveLock(pagedKey);
      try {
        PageGroup pageGroup = writeCachePages.get(pagedKey);
        if (pageGroup == null) {
          pageGroup = new PageGroup(System.currentTimeMillis(), dataPointer);

          writeCachePages.put(pagedKey, pageGroup);

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
        future = commitExecutor.submit(new PeriodicFlushTask());
      }

      return future;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public OCachePointer load(long fileId, long pageIndex, boolean addNewPages) throws IOException {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      final PagedKey pagedKey = new PagedKey(intId, pageIndex);
      Lock groupLock = lockManager.acquireSharedLock(pagedKey);
      try {
        PageGroup pageGroup = writeCachePages.get(pagedKey);

        OCachePointer pagePointer;
        if (pageGroup == null) {
          pagePointer = cacheFileContent(fileId, intId, pageIndex, addNewPages);
          if (pagePointer == null)
            return null;

          pagePointer.incrementReadersReferrer();
          return pagePointer;
        }

        pagePointer = pageGroup.page;
        pagePointer.incrementReadersReferrer();
        return pagePointer;
      } finally {
        lockManager.releaseLock(groupLock);
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public void addOnlyWriters(long fileId, long pageIndex) {
    exclusiveWriteCacheSize.increment();
    exclusiveWritePages.add(new PagedKey(extractFileId(fileId), pageIndex));
  }

  @Override
  public void removeOnlyWriters(long fileId, long pageIndex) {
    exclusiveWriteCacheSize.decrement();
    exclusiveWritePages.remove(new PagedKey(extractFileId(fileId), pageIndex));
  }

  public void flush(long fileId) {
    final Future<Void> future = commitExecutor.submit(new FileFlushTask(extractFileId(fileId)));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OException("File flush was interrupted", e);
    } catch (Exception e) {
      throw new OException("File flush was abnormally terminated", e);
    }
  }

  public void flush() {
    for (long fileId : files.keySet())
      flush(fileId);
  }

  public long getFilledUpTo(long fileId) throws IOException {
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

  public boolean isOpen(long fileId) {
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

  public Long isOpen(String fileName) throws IOException {
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

  public void deleteFile(long fileId) throws IOException {
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
        final File newFile = new File(storageLocal.getStoragePath() + File.separator + newFileName
            + osFileName.substring(osFileName.lastIndexOf(oldFileName) + oldFileName.length()));
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
          throw new OException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OException("Data flush thread was interrupted", e);
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
      for (int fileId : files.keySet()) {

        OFileClassic fileClassic = files.get(fileId);

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
                commandOutputListener.onMessage("Error: Magic number for page " + (pos / pageSize) + " in file "
                    + fileClassic.getName() + " does not much !!!");
              fileIsCorrect = false;
            }

            final int storedCRC32 = OIntegerSerializer.INSTANCE.deserializeNative(data, OLongSerializer.LONG_SIZE);

            final int calculatedCRC32 = calculatePageCrc(data);
            if (storedCRC32 != calculatedCRC32) {
              checkSumIncorrect = true;
              if (commandOutputListener != null)
                commandOutputListener.onMessage("Error: Checksum for page " + (pos / pageSize) + " in file '"
                    + fileClassic.getName() + "' is incorrect !!!");
              fileIsCorrect = false;
            }

            if (magicNumberIncorrect || checkSumIncorrect)
              errors.add(new OPageDataVerificationError(magicNumberIncorrect, checkSumIncorrect, pos / pageSize, fileClassic
                  .getName()));

            if (commandOutputListener != null && System.currentTimeMillis() - time > notificationTimeOut) {
              time = notificationTimeOut;
              commandOutputListener.onMessage((pos / pageSize) + " pages were processed ...");
            }
          }
        } catch (IOException ioe) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Error: Error during processing of file '" + fileClassic.getName() + "'. "
                + ioe.getMessage());

          fileIsCorrect = false;
        }

        if (!fileIsCorrect) {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file '" + fileClassic.getName() + "' is finished with errors.");
        } else {
          if (commandOutputListener != null)
            commandOutputListener.onMessage("Verification of file '" + fileClassic.getName() + "' is successfully finished.");
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
          throw new OException("Background data flush task cannot be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OException("Data flush thread was interrupted", e);
      }
    }

    return result;
  }

  public String fileNameById(long fileId) {
    final int intId = extractFileId(fileId);

    filesLock.acquireReadLock();
    try {
      return files.get(intId).getName();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  @Override
  public int getId() {
    return id;
  }

  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName());
        server.registerMBean(this, mbeanName);
      } catch (MalformedObjectNameException e) {
        throw new OStorageException("Error during registration of write cache MBean.", e);
      } catch (InstanceAlreadyExistsException e) {
        throw new OStorageException("Error during registration of write cache MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during registration of write cache MBean.", e);
      } catch (NotCompliantMBeanException e) {
        throw new OStorageException("Error during registration of write cache MBean.", e);
      }
    }
  }

  private String getMBeanName() {
    return MBEAN_NAME + ",name=" + ObjectName.quote(storageLocal.getName()) + ",id=" + storageLocal.getId();
  }

  public void unregisterMBean() {
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName());
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException e) {
        throw new OStorageException("Error during unregistration of write cache MBean.", e);
      } catch (InstanceNotFoundException e) {
        throw new OStorageException("Error during unregistration of write cache MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during unregistration of write cache MBean.", e);
      }
    }
  }

  @Override
  public long getWriteCacheSize() {
    return writeCacheSize.get();
  }

  @Override
  public long getWriteCacheSizeInMB() {
    return getWriteCacheSize() / (1024 * 1024);
  }

  @Override
  public double getWriteCacheSizeInGB() {
    return Math.ceil((getWriteCacheSize() * 100.0) / (1024 * 1204 * 1024)) / 100;
  }

  @Override
  public long getExclusiveWriteCacheSize() {
    return exclusiveWriteCacheSize.get();
  }

  @Override
  public long getExclusiveWriteCacheSizeInMB() {
    return getExclusiveWriteCacheSize() / (1024 * 1024);
  }

  @Override
  public double getExclusiveWriteCacheSizeInGB() {
    return Math.ceil((getExclusiveWriteCacheSize() * 100.0) / (1024 * 1024 * 1024)) / 100;
  }

  @Override
  public Date getLastFuzzyCheckpointDate() {
    return lastFuzzyCheckpointDate.get();
  }

  @Override
  public long getLastAmountOfFlushedPages() {
    return lastAmountOfFlushedPages.get();
  }

  @Override
  public long getDurationOfLastFlush() {
    return durationOfLastFlush.get();
  }

  private void openFile(final OFileClassic fileClassic) throws IOException {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen())
        fileClassic.open();
    } else {
      throw new OStorageException("File '" + fileClassic + "' does not exist.");
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
    byte[] serializedName = new byte[nameSize];
    stringSerializer.serialize(nameFileIdEntry.name, serializedName, 0);

    nameIdMapHolder.writeInt(nameSize);
    nameIdMapHolder.write(serializedName);
    nameIdMapHolder.writeLong(nameFileIdEntry.fileId);

    if (sync)
      nameIdMapHolder.getFD().sync();
  }

  private String doDeleteFile(int fileId) throws IOException {
    if (isOpen(fileId))
      truncateFile(fileId);

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
      throw new OException("File data removal was interrupted", e);
    } catch (Exception e) {
      throw new OException("File data removal was abnormally terminated", e);
    }
  }

  private OCachePointer cacheFileContent(long fileId, int intId, long pageIndex, boolean addNewPages) throws IOException {
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize + 2 * PAGE_PADDING];
    OCachePointer dataPointer;
    final OFileClassic fileClassic = files.get(intId);

    if (fileClassic == null)
      throw new IllegalArgumentException("File with id " + intId + " not found in WOW Cache");

    OLogSequenceNumber lastLsn;
    if (writeAheadLog != null)
      lastLsn = writeAheadLog.getFlushedLSN();
    else
      lastLsn = new OLogSequenceNumber(-1, -1);

    if (fileClassic.getFileSize() >= endPosition) {
      fileClassic.read(startPosition, content, content.length - 2 * PAGE_PADDING, PAGE_PADDING);
      final ODirectMemoryPointer pointer = new ODirectMemoryPointer(content);

      dataPointer = new OCachePointer(pointer, lastLsn, fileId, pageIndex);
    } else if (addNewPages) {
      final int space = (int) (endPosition - fileClassic.getFileSize());
      fileClassic.allocateSpace(space);

      addAllocatedSpace(space);

      final ODirectMemoryPointer pointer = new ODirectMemoryPointer(content);
      dataPointer = new OCachePointer(pointer, lastLsn, fileId, pageIndex);
    } else
      return null;

    return dataPointer;
  }

  private void flushPage(int fileId, long pageIndex, ODirectMemoryPointer dataPointer) throws IOException {
    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = ODurablePage.getLogSequenceNumberFromPage(dataPointer);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = dataPointer.get(PAGE_PADDING, pageSize);
    OLongSerializer.INSTANCE.serializeNative(MAGIC_NUMBER, content, 0);

    final int crc32 = calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(fileId);

    final long spaceDiff = fileClassic.write(pageIndex * pageSize, content);

    assert spaceDiff >= 0;

    addAllocatedSpace(-spaceDiff);

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

  private static final class PagedKey implements Comparable<PagedKey> {
    private final int  fileId;
    private final long pageIndex;

    private PagedKey(int fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int compareTo(PagedKey other) {
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
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PagedKey pagedKey = (PagedKey) o;

      if (fileId != pagedKey.fileId)
        return false;
      if (pageIndex != pagedKey.pageIndex)
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
      return "PagedKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }
  }

  private final class PeriodicFlushTask implements Runnable {

    @Override
    public void run() {
      final long start = System.currentTimeMillis();
      try {
        if (writeCachePages.isEmpty()) {
          lastAmountOfFlushedPages.lazySet(0);
          return;
        }

        int writePagesToFlush = 0;

        final long wcs = exclusiveWriteCacheSize.get();
        final long cs = writeCacheSize.get();

        boolean iterateByWritePagesFirst = false;
        boolean forceFlush = false;

        double writeCacheThreshold = ((double) wcs) / writeCacheMaxSize;

        if (writeCacheThreshold > 0.3) {
          writePagesToFlush = (int) Math.floor(((writeCacheThreshold - 0.3) / 0.4) * MAX_PAGES_PER_FLUSH);
          iterateByWritePagesFirst = true;

          if (writeCacheThreshold > 0.7)
            forceFlush = true;
        }

        double cacheThreshold = ((double) cs) / cacheMaxSize;
        if (cacheThreshold > 0.3) {
          final int pagesToFlush = (int) Math.floor(((cacheThreshold - 0.3) / 0.4) * MAX_PAGES_PER_FLUSH);

          writePagesToFlush = Math.max(pagesToFlush, writePagesToFlush);
          if (cacheThreshold > 0.7)
            forceFlush = true;
        }

        writePagesToFlush = Math.max(4, Math.min(MAX_PAGES_PER_FLUSH, writePagesToFlush));

        int flushedPages = 0;

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

        lastAmountOfFlushedPages.lazySet(flushedPages);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during data flush.", e);
      } finally {
        final long end = System.currentTimeMillis();
        durationOfLastFlush.lazySet(end - start);
      }
    }

    private int flushRing(int writePagesToFlush, int flushedPages, boolean forceFlush, boolean iterateByWritePagesFirst)
        throws IOException {

      NavigableMap<PagedKey, PageGroup> subMap = null;
      NavigableSet<PagedKey> writePagesSubset = null;

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

    private int iterateBySubRing(NavigableMap<PagedKey, PageGroup> subMap, NavigableSet<PagedKey> subSet, int writePagesToFlush,
        int flushedWritePages, boolean forceFlush, boolean iterateByWritePagesFirst) throws IOException {
      if (!iterateByWritePagesFirst) {
        return iterateByCacheSubRing(subMap, writePagesToFlush, flushedWritePages, forceFlush);
      } else {
        return iterateByWritePagesSubRing(subSet, writePagesToFlush, flushedWritePages, forceFlush);
      }
    }

    private int iterateByWritePagesSubRing(NavigableSet<PagedKey> subSet, int writePagesToFlush, int flushedWritePages,
        boolean forceFlush) throws IOException {
      Iterator<PagedKey> entriesIterator = subSet.iterator();
      long currentTime = System.currentTimeMillis();

      int flushedRegions = 0;

      long lastPageIndex = -1;
      while (entriesIterator.hasNext()) {
        PagedKey entry = entriesIterator.next();
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

          final boolean weakLockMode = group.creationTime - currentTime < groupTTL && !forceFlush;
          if (group.recencyBit && weakLockMode)
            group.recencyBit = false;
          else {
            group.recencyBit = false;

            final OCachePointer pagePointer = group.page;

            if (!pagePointer.tryAcquireSharedLock())
              continue;

            try {
              flushPage(entry.fileId, entry.pageIndex, pagePointer.getDataPointer());

              final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(pagePointer.getDataPointer());
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

    private int iterateByCacheSubRing(NavigableMap<PagedKey, PageGroup> subMap, int writePagesToFlush, int flushedWritePages,
        boolean forceFlush) throws IOException {
      Iterator<Map.Entry<PagedKey, PageGroup>> entriesIterator = subMap.entrySet().iterator();
      long currentTime = System.currentTimeMillis();

      int flushedRegions = 0;

      long lastPageIndex = -1;
      while (entriesIterator.hasNext()) {
        Map.Entry<PagedKey, PageGroup> entry = entriesIterator.next();

        final PageGroup group = entry.getValue();
        final PagedKey pagedKey = entry.getKey();

        if (lastPageIndex >= 0) {
          if (pagedKey.pageIndex != lastPageIndex + 1) {
            flushedRegions++;

            if (flushedWritePages > writePagesToFlush && flushedRegions >= 4)
              break;
          }
        }

        final boolean weakLockMode = group.creationTime - currentTime < groupTTL && !forceFlush;
        if (group.recencyBit && weakLockMode) {
          group.recencyBit = false;
          continue;
        }

        Lock groupLock = lockManager.acquireExclusiveLock(entry.getKey());
        try {
          if (group.recencyBit && weakLockMode)
            group.recencyBit = false;
          else {
            group.recencyBit = false;

            final OCachePointer pagePointer = group.page;

            if (!pagePointer.tryAcquireSharedLock())
              continue;

            try {
              flushPage(pagedKey.fileId, pagedKey.pageIndex, pagePointer.getDataPointer());

              final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(pagePointer.getDataPointer());
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

        lastPageKey = pagedKey;

        flushedWritePages++;
        lastPageIndex = pagedKey.pageIndex;

        writeCacheSize.decrement();
      }

      return flushedWritePages;
    }
  }

  private final class PeriodicalFuzzyCheckpointTask implements Runnable {
    private PeriodicalFuzzyCheckpointTask() {
    }

    @Override
    public void run() {
      OLogSequenceNumber minLsn = writeAheadLog.getFlushedLSN();

      minLsn = findMinLsn(minLsn, writeCachePages);

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
      lastFuzzyCheckpointDate.lazySet(new Date());
    }

    private OLogSequenceNumber findMinLsn(OLogSequenceNumber minLsn, ConcurrentSkipListMap<PagedKey, PageGroup> ring) {
      for (Map.Entry<PagedKey, PageGroup> entry : ring.entrySet()) {
        Lock groupLock = lockManager.acquireExclusiveLock(entry.getKey());
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

    private FileFlushTask(int fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final PagedKey firstKey = new PagedKey(fileId, 0);
      final PagedKey lastKey = new PagedKey(fileId, Long.MAX_VALUE);

      flushRing(writeCachePages.subMap(firstKey, true, lastKey, true));

      files.get(fileId).synch();
      return null;
    }

    private void flushRing(NavigableMap<PagedKey, PageGroup> subMap) throws IOException {
      Iterator<Map.Entry<PagedKey, PageGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<PagedKey, PageGroup> entry = entryIterator.next();
        final PageGroup pageGroup = entry.getValue();
        final PagedKey pagedKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(pagedKey);
        try {

          final OCachePointer pagePointer = pageGroup.page;

          if (!pagePointer.tryAcquireSharedLock())
            continue;

          try {
            flushPage(pagedKey.fileId, pagedKey.pageIndex, pagePointer.getDataPointer());
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
      final PagedKey firstKey = new PagedKey(fileId, 0);
      final PagedKey lastKey = new PagedKey(fileId, Long.MAX_VALUE);

      removeFromRing(writeCachePages.subMap(firstKey, true, lastKey, true));

      return null;
    }

    private void removeFromRing(NavigableMap<PagedKey, PageGroup> subMap) {
      Iterator<Map.Entry<PagedKey, PageGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<PagedKey, PageGroup> entry = entryIterator.next();
        PageGroup pageGroup = entry.getValue();
        PagedKey pagedKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(pagedKey);
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
