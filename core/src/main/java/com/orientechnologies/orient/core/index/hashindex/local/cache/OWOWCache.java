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
package com.orientechnologies.orient.core.index.hashindex.local.cache;

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
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OWOWCache {
  // we add 8 bytes before and after cache pages to prevent word tearing in mt case.
  public static final int                                   PAGE_PADDING          = 8;

  public static final String                                NAME_ID_MAP_EXTENSION = ".cm";

  private static final String                               NAME_ID_MAP           = "name_id_map" + NAME_ID_MAP_EXTENSION;

  public static final int                                   MIN_CACHE_SIZE        = 16;

  public static final long                                  MAGIC_NUMBER          = 0xFACB03FEL;

  private final long                                        freeSpaceLimit        = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT
                                                                                      .getValueAsLong() * 1024L * 1024L;

  private final long                                        diskSizeCheckInterval = OGlobalConfiguration.DISC_CACHE_FREE_SPACE_CHECK_INTERVAL
                                                                                      .getValueAsInteger() * 1000;
  private final List<WeakReference<OLowDiskSpaceListener>>  listeners             = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();

  private final AtomicLong                                  lastDiskSpaceCheck    = new AtomicLong(System.currentTimeMillis());
  private final String                                      storagePath;

  private final ConcurrentSkipListMap<GroupKey, WriteGroup> writeGroups           = new ConcurrentSkipListMap<GroupKey, WriteGroup>();
  private final OBinarySerializer<String>                   stringSerializer;
  private final Map<Long, OFileClassic>                     files;
  private final boolean                                     syncOnPageFlush;
  private final int                                         pageSize;
  private final long                                        groupTTL;
  private final OWriteAheadLog                              writeAheadLog;
  private final AtomicInteger                               cacheSize             = new AtomicInteger();
  private final ONewLockManager<GroupKey>                   lockManager           = new ONewLockManager<GroupKey>();
  private final OLocalPaginatedStorage                      storageLocal;
  private final OReadersWriterSpinLock                      filesLock             = new OReadersWriterSpinLock();
  private final ScheduledExecutorService                    commitExecutor;

  private final ExecutorService                             lowSpaceEventsPublisher;

  private Map<String, Long>                                 nameIdMap;
  private RandomAccessFile                                  nameIdMapHolder;
  private volatile int                                      cacheMaxSize;
  private long                                              fileCounter           = 0;
  private GroupKey                                          lastGroupKey          = new GroupKey(0, -1);
  private File                                              nameIdMapHolderFile;

  private final AtomicLong                                  allocatedSpace        = new AtomicLong();

  public OWOWCache(boolean syncOnPageFlush, int pageSize, long groupTTL, OWriteAheadLog writeAheadLog, long pageFlushInterval,
      int cacheMaxSize, OLocalPaginatedStorage storageLocal, boolean checkMinSize) {
    filesLock.acquireWriteLock();
    try {
      this.files = new ConcurrentHashMap<Long, OFileClassic>();

      this.syncOnPageFlush = syncOnPageFlush;
      this.pageSize = pageSize;
      this.groupTTL = groupTTL;
      this.writeAheadLog = writeAheadLog;
      this.cacheMaxSize = cacheMaxSize;
      this.storageLocal = storageLocal;

      this.storagePath = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath());

      final OBinarySerializerFactory binarySerializerFactory = storageLocal.getComponentsFactory().binarySerializerFactory;
      this.stringSerializer = binarySerializerFactory.getObjectSerializer(OType.STRING);

      if (checkMinSize && this.cacheMaxSize < MIN_CACHE_SIZE)
        this.cacheMaxSize = MIN_CACHE_SIZE;

      commitExecutor = Executors.newSingleThreadScheduledExecutor(new FlushThreadFactory(storageLocal.getName()));
      lowSpaceEventsPublisher = Executors.newCachedThreadPool(new LowSpaceEventsPublisherFactory(storageLocal.getName()));

      if (pageFlushInterval > 0)
        commitExecutor.scheduleWithFixedDelay(new PeriodicFlushTask(), pageFlushInterval, pageFlushInterval, TimeUnit.MILLISECONDS);

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void startFuzzyCheckpoints() {
    if (writeAheadLog != null) {
      final long fuzzyCheckPointInterval = OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.getValueAsInteger();
      commitExecutor.scheduleWithFixedDelay(new PeriodicalFuzzyCheckpointTask(), fuzzyCheckPointInterval, fuzzyCheckPointInterval,
          TimeUnit.SECONDS);
    }
  }

  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    listeners.add(new WeakReference<OLowDiskSpaceListener>(listener));
  }

  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<WeakReference<OLowDiskSpaceListener>>();

    for (WeakReference<OLowDiskSpaceListener> ref : listeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OLowDiskSpaceListener> ref : itemsToRemove)
      listeners.remove(ref);
  }

  private void addAllocatedSpace(long diff) {
    if (diff == 0)
      return;

    allocatedSpace.addAndGet(diff);

    final long ts = System.currentTimeMillis();
    final long lastSpaceCheck = lastDiskSpaceCheck.get();

    if (ts - lastSpaceCheck > diskSizeCheckInterval) {
      final File storageDir = new File(storagePath);

      long freeSpace = storageDir.getFreeSpace();
      long effectiveFreeSpace = freeSpace - allocatedSpace.get();

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

  private static int calculatePageCrc(byte[] pageData) {
    int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  public long bookFileId(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();
      Long fileId = nameIdMap.get(fileName);

      if (fileId != null && fileId < 0)
        return -fileId;

      ++fileCounter;
      return fileCounter;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long openFile(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      Long fileId = nameIdMap.get(fileName);
      OFileClassic fileClassic;

      if (fileId == null || fileId < 0)
        fileClassic = null;
      else
        fileClassic = files.get(fileId);

      if (fileClassic == null)
        throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageLocal.getName());

      openFile(fileClassic);

      return fileId;

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public long addFile(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      Long fileId = nameIdMap.get(fileName);
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

      return fileId;

    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void openFile(String fileName, long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      OFileClassic fileClassic;

      Long existingFileId = nameIdMap.get(fileName);

      if (existingFileId != null && fileId >= 0) {
        if (existingFileId == fileId)
          fileClassic = files.get(fileId);
        else
          throw new OStorageException("File with given name already exists but has different id " + existingFileId
              + " vs. proposed " + fileId);
      } else {
        throw new OStorageException("File with name " + fileName + " does not exist in storage " + storageLocal.getName());
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

      Long existingFileId = nameIdMap.get(fileName);

      if (existingFileId != null && existingFileId >= 0) {
        if (existingFileId == fileId)
          throw new OStorageException("File with name " + fileName + " already exists in storage " + storageLocal.getName());
        else
          throw new OStorageException("File with given name already exists but has different id " + existingFileId
              + " vs. proposed " + fileId);
      }

      fileClassic = files.get(fileId);

      if (fileClassic != null)
        throw new OStorageException("File with given id exists but has different name " + fileClassic.getName() + " vs. proposed "
            + fileName);

      if (fileCounter < fileId)
        fileCounter = fileId;

      fileClassic = createFile(fileName);

      files.put(fileId, fileClassic);
      nameIdMap.put(fileName, fileId);
      writeNameIdEntry(new NameFileIdEntry(fileName, fileId), true);

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
        throw new OStorageException("Error during fuzzy checkpoint execution for storage " + storageLocal.getName(), e);
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

      final OFileClassic fileClassic = files.get(fileId);
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
        Long fileId = nameIdMap.get(fileName);
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

    filesLock.acquireReadLock();
    try {
      final GroupKey groupKey = new GroupKey(fileId, pageIndex >>> 4);
      Lock groupLock = lockManager.acquireExclusiveLock(groupKey);
      try {
        WriteGroup writeGroup = writeGroups.get(groupKey);
        if (writeGroup == null) {
          writeGroup = new WriteGroup(System.currentTimeMillis());
          writeGroups.put(groupKey, writeGroup);
        }

        int entryIndex = (int) (pageIndex & 15);

        if (writeGroup.pages[entryIndex] == null) {
          dataPointer.incrementReferrer();
          writeGroup.pages[entryIndex] = dataPointer;

          cacheSize.incrementAndGet();
        } else {
          if (!writeGroup.pages[entryIndex].equals(dataPointer)) {
            writeGroup.pages[entryIndex].decrementReferrer();
            dataPointer.incrementReferrer();

            writeGroup.pages[entryIndex] = dataPointer;
          }
        }

        writeGroup.recencyBit = true;
      } finally {
        lockManager.releaseLock(groupLock);
      }

      if (cacheSize.get() > cacheMaxSize) {
        future = commitExecutor.submit(new PeriodicFlushTask());
      }

      return future;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public OCachePointer load(long fileId, long pageIndex, boolean addNewPages) throws IOException {
    filesLock.acquireReadLock();
    try {
      final GroupKey groupKey = new GroupKey(fileId, pageIndex >>> 4);
      Lock groupLock = lockManager.acquireSharedLock(groupKey);
      try {
        final WriteGroup writeGroup = writeGroups.get(groupKey);

        OCachePointer pagePointer;
        if (writeGroup == null) {
          pagePointer = cacheFileContent(fileId, pageIndex, addNewPages);
          if (pagePointer == null)
            return null;

          pagePointer.incrementReferrer();
          return pagePointer;
        }

        final int entryIndex = (int) (pageIndex & 15);
        pagePointer = writeGroup.pages[entryIndex];

        if (pagePointer == null)
          pagePointer = cacheFileContent(fileId, pageIndex, addNewPages);

        if (pagePointer == null)
          return null;

        pagePointer.incrementReferrer();
        return pagePointer;
      } finally {
        lockManager.releaseLock(groupLock);
      }
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public void flush(long fileId) {
    final Future<Void> future = commitExecutor.submit(new FileFlushTask(fileId));
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
    filesLock.acquireReadLock();
    try {
      return files.get(fileId).getFilledUpTo() / pageSize;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public long getAllocatedPages() {
    return cacheSize.get();
  }

  public boolean isOpen(long fileId) {
    filesLock.acquireReadLock();
    try {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic != null)
        return fileClassic.isOpen();

      return false;
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public long isOpen(String fileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      initNameIdMapping();

      final Long fileId = nameIdMap.get(fileName);
      if (fileId == null || fileId < 0)
        return -1;

      final OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null || !fileClassic.isOpen())
        return -1;

      return fileId;
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
    filesLock.acquireWriteLock();
    try {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic != null && fileClassic.isOpen())
        fileClassic.setSoftlyClosed(softlyClosed);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    filesLock.acquireWriteLock();
    try {
      for (long fileId : files.keySet())
        setSoftlyClosed(fileId, softlyClosed);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public boolean wasSoftlyClosed(long fileId) throws IOException {
    filesLock.acquireReadLock();
    try {
      OFileClassic fileClassic = files.get(fileId);
      if (fileClassic == null)
        return false;

      return fileClassic.wasSoftlyClosed();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  public void deleteFile(long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      final String name = doDeleteFile(fileId);
      if (name != null) {
        nameIdMap.put(name, -fileId);
        writeNameIdEntry(new NameFileIdEntry(name, -fileId), true);
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void truncateFile(long fileId) throws IOException {
    filesLock.acquireWriteLock();
    try {
      removeCachedPages(fileId);
      files.get(fileId).shrink(0);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    filesLock.acquireWriteLock();
    try {
      if (!files.containsKey(fileId))
        return;

      final OFileClassic file = files.get(fileId);
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
      nameIdMap.put(newFileName, fileId);

      writeNameIdEntry(new NameFileIdEntry(oldFileName, -1), false);
      writeNameIdEntry(new NameFileIdEntry(newFileName, fileId), true);
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void close() throws IOException {
    flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OException("Background data flush task can not be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OException("Data flush thread was interrupted", e);
      }
    }

    filesLock.acquireWriteLock();
    try {
      for (OFileClassic fileClassic : files.values()) {
        if (fileClassic.isOpen())
          fileClassic.close();
      }

      if (nameIdMapHolder != null) {
        nameIdMapHolder.setLength(0);
        for (Map.Entry<String, Long> entry : nameIdMap.entrySet()) {
          writeNameIdEntry(new NameFileIdEntry(entry.getKey(), entry.getValue()), false);
        }
        nameIdMapHolder.getFD().sync();
        nameIdMapHolder.close();
      }
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public void close(long fileId, boolean flush) throws IOException {
    filesLock.acquireWriteLock();
    try {
      if (!isOpen(fileId))
        return;

      if (flush)
        flush(fileId);
      else
        removeCachedPages(fileId);

      files.get(fileId).close();
    } finally {
      filesLock.releaseWriteLock();
    }
  }

  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    final int notificationTimeOut = 5000;
    final List<OPageDataVerificationError> errors = new ArrayList<OPageDataVerificationError>();

    filesLock.acquireWriteLock();
    try {
      for (long fileId : files.keySet()) {

        OFileClassic fileClassic = files.get(fileId);

        boolean fileIsCorrect;
        try {

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Flashing file " + fileClassic.getName() + "... ");

          flush(fileId);

          if (commandOutputListener != null)
            commandOutputListener.onMessage("Start verification of content of " + fileClassic.getName() + "file ...");

          long time = System.currentTimeMillis();

          long filledUpTo = fileClassic.getFilledUpTo();
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
                commandOutputListener.onMessage("Error: Checksum for page " + (pos / pageSize) + " in file "
                    + fileClassic.getName() + " is incorrect !!!");
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
            commandOutputListener.onMessage("Error: Error during processing of file " + fileClassic.getName() + ". "
                + ioe.getMessage());

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

  public void delete() throws IOException {
    filesLock.acquireWriteLock();
    try {
      for (long fileId : files.keySet())
        doDeleteFile(fileId);

      if (nameIdMapHolderFile != null) {
        if (nameIdMapHolderFile.exists()) {
          nameIdMapHolder.close();

          if (!nameIdMapHolderFile.delete())
            throw new OStorageException("Can not delete disk cache file which contains name-id mapping.");
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
          throw new OException("Background data flush task can not be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OException("Data flush thread was interrupted", e);
      }
    }
  }

  public String fileNameById(long fileId) {
    filesLock.acquireReadLock();
    try {
      return files.get(fileId).getName();
    } finally {
      filesLock.releaseReadLock();
    }
  }

  private void openFile(OFileClassic fileClassic) throws IOException {
    if (fileClassic.exists()) {
      if (!fileClassic.isOpen())
        fileClassic.open();
    } else {
      throw new OStorageException("File " + fileClassic + " does not exist.");
    }

  }

  private void addFile(OFileClassic fileClassic) throws IOException {
    if (!fileClassic.exists()) {
      fileClassic.create(-1);
      fileClassic.synch();
    } else {
      throw new OStorageException("File " + fileClassic + " already exists.");
    }
  }

  private void initNameIdMapping() throws IOException {
    if (nameIdMapHolder == null) {
      final File storagePath = new File(storageLocal.getStoragePath());
      if (!storagePath.exists())
        if (!storagePath.mkdirs())
          throw new OStorageException("Can not create directories for the path " + storagePath);

      nameIdMapHolderFile = new File(storagePath, NAME_ID_MAP);

      nameIdMapHolder = new RandomAccessFile(nameIdMapHolderFile, "rw");
      readNameIdMap();
    }
  }

  private OFileClassic createFile(String fileName) {
    OFileClassic fileClassic = new OFileClassic();
    String path = storageLocal.getVariableParser().resolveVariables(storageLocal.getStoragePath() + File.separator + fileName);
    fileClassic.init(path, storageLocal.getMode());
    return fileClassic;
  }

  private void readNameIdMap() throws IOException {
    nameIdMap = new ConcurrentHashMap<String, Long>();
    long localFileCounter = -1;

    nameIdMapHolder.seek(0);

    NameFileIdEntry nameFileIdEntry;
    while ((nameFileIdEntry = readNextNameIdEntry()) != null) {
      if (localFileCounter < nameFileIdEntry.fileId)
        localFileCounter = nameFileIdEntry.fileId;

      nameIdMap.put(nameFileIdEntry.name, nameFileIdEntry.fileId);
    }

    if (localFileCounter > 0)
      fileCounter = localFileCounter;

    for (Map.Entry<String, Long> nameIdEntry : nameIdMap.entrySet()) {
      if (nameIdEntry.getValue() >= 0 && !files.containsKey(nameIdEntry.getValue())) {
        OFileClassic fileClassic = createFile(nameIdEntry.getKey());
        files.put(nameIdEntry.getValue(), fileClassic);
      }
    }
  }

  private NameFileIdEntry readNextNameIdEntry() throws IOException {
    try {
      final int nameSize = nameIdMapHolder.readInt();
      byte[] serializedName = new byte[nameSize];

      nameIdMapHolder.readFully(serializedName);

      final String name = stringSerializer.deserialize(serializedName, 0);
      final long fileId = nameIdMapHolder.readLong();

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

  private String doDeleteFile(long fileId) throws IOException {
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

  private void removeCachedPages(long fileId) {
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

  private OCachePointer cacheFileContent(long fileId, long pageIndex, boolean addNewPages) throws IOException {
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize + 2 * PAGE_PADDING];
    OCachePointer dataPointer;
    final OFileClassic fileClassic = files.get(fileId);

    if (fileClassic == null)
      throw new IllegalArgumentException("File with id " + fileId + " not found in WOW Cache");

    OLogSequenceNumber lastLsn;
    if (writeAheadLog != null)
      lastLsn = writeAheadLog.getFlushedLSN();
    else
      lastLsn = new OLogSequenceNumber(-1, -1);

    if (fileClassic.getFilledUpTo() >= endPosition) {
      fileClassic.read(startPosition, content, content.length - 2 * PAGE_PADDING, PAGE_PADDING);
      final ODirectMemoryPointer pointer = new ODirectMemoryPointer(content);

      dataPointer = new OCachePointer(pointer, lastLsn);
    } else if (addNewPages) {
      final int space = (int) (endPosition - fileClassic.getFilledUpTo());
      fileClassic.allocateSpace(space);

      addAllocatedSpace(space);

      final ODirectMemoryPointer pointer = new ODirectMemoryPointer(content);
      dataPointer = new OCachePointer(pointer, lastLsn);
    } else
      return null;

    return dataPointer;
  }

  private void flushPage(long fileId, long pageIndex, ODirectMemoryPointer dataPointer) throws IOException {
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
    private final long   fileId;

    private NameFileIdEntry(String name, long fileId) {
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
      result = 31 * result + (int) (fileId ^ (fileId >>> 32));
      return result;
    }
  }

  private final class GroupKey implements Comparable<GroupKey> {
    private final long fileId;
    private final long groupIndex;

    private GroupKey(long fileId, long groupIndex) {
      this.fileId = fileId;
      this.groupIndex = groupIndex;
    }

    @Override
    public int compareTo(GroupKey other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (groupIndex > other.groupIndex)
        return 1;
      if (groupIndex < other.groupIndex)
        return -1;

      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      GroupKey groupKey = (GroupKey) o;

      if (fileId != groupKey.fileId)
        return false;
      if (groupIndex != groupKey.groupIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (groupIndex ^ (groupIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "GroupKey{" + "fileId=" + fileId + ", groupIndex=" + groupIndex + '}';
    }
  }

  private final class PeriodicFlushTask implements Runnable {

    @Override
    public void run() {
      try {
        if (writeGroups.isEmpty())
          return;

        int writeGroupsToFlush;
        boolean useForceSync = false;
        double threshold = ((double) cacheSize.get()) / cacheMaxSize;
        if (threshold > 0.8) {
          writeGroupsToFlush = (int) (0.2 * writeGroups.size());
          useForceSync = true;
        } else if (threshold > 0.9) {
          writeGroupsToFlush = (int) (0.4 * writeGroups.size());
          useForceSync = true;
        } else
          writeGroupsToFlush = 1;

        if (writeGroupsToFlush < 1)
          writeGroupsToFlush = 1;

        int flushedGroups = 0;

        flushedGroups = flushRing(writeGroupsToFlush, flushedGroups, false);

        if (flushedGroups < writeGroupsToFlush && useForceSync)
          flushedGroups = flushRing(writeGroupsToFlush, flushedGroups, true);

        if (flushedGroups < writeGroupsToFlush && cacheSize.get() > cacheMaxSize) {
          if (OGlobalConfiguration.SERVER_CACHE_INCREASE_ON_DEMAND.getValueAsBoolean()) {
            final long oldCacheMaxSize = cacheMaxSize;

            cacheMaxSize = (int) Math.ceil(cacheMaxSize * (1 + OGlobalConfiguration.SERVER_CACHE_INCREASE_STEP.getValueAsFloat()));
            OLogManager.instance().warn(this, "Write cache size is increased from %d to %d", oldCacheMaxSize, cacheMaxSize);
          } else {
            throw new OAllCacheEntriesAreUsedException("All records in write cache are used!");
          }
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during data flush.", e);
      }
    }

    private int flushRing(int writeGroupsToFlush, int flushedGroups, boolean forceFlush) throws IOException {
      NavigableMap<GroupKey, WriteGroup> subMap = writeGroups.tailMap(lastGroupKey, false);

      if (!subMap.isEmpty()) {
        flushedGroups = iterateBySubRing(subMap, writeGroupsToFlush, 0, forceFlush);
        if (flushedGroups < writeGroupsToFlush) {
          if (!subMap.isEmpty()) {
            subMap = writeGroups.headMap(subMap.firstKey(), false);
            flushedGroups = iterateBySubRing(subMap, writeGroupsToFlush, flushedGroups, forceFlush);
          }
        }
      } else
        flushedGroups = iterateBySubRing(writeGroups, writeGroupsToFlush, flushedGroups, forceFlush);

      return flushedGroups;
    }

    private int iterateBySubRing(NavigableMap<GroupKey, WriteGroup> subMap, int writeGroupsToFlush, int flushedWriteGroups,
        boolean forceFlush) throws IOException {
      Iterator<Map.Entry<GroupKey, WriteGroup>> entriesIterator = subMap.entrySet().iterator();
      long currentTime = System.currentTimeMillis();

      groupsLoop: while (entriesIterator.hasNext() && flushedWriteGroups < writeGroupsToFlush) {
        Map.Entry<GroupKey, WriteGroup> entry = entriesIterator.next();
        final WriteGroup group = entry.getValue();
        final GroupKey groupKey = entry.getKey();

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

            int flushedPages = 0;

            for (int i = 0; i < 16; i++) {
              final OCachePointer pagePointer = group.pages[i];
              if (pagePointer != null) {
                if (!pagePointer.tryAcquireSharedLock())
                  continue groupsLoop;

                try {
                  flushPage(groupKey.fileId, (groupKey.groupIndex << 4) + i, pagePointer.getDataPointer());
                  flushedPages++;

                  final OLogSequenceNumber flushedLSN = ODurablePage.getLogSequenceNumberFromPage(pagePointer.getDataPointer());
                  pagePointer.setLastFlushedLsn(flushedLSN);
                } finally {
                  pagePointer.releaseSharedLock();
                }
              }
            }

            for (OCachePointer pagePointer : group.pages)
              if (pagePointer != null)
                pagePointer.decrementReferrer();

            entriesIterator.remove();
            flushedWriteGroups++;

            cacheSize.addAndGet(-flushedPages);
          }
        } finally {
          lockManager.releaseLock(groupLock);
        }

        lastGroupKey = groupKey;
      }

      return flushedWriteGroups;
    }
  }

  private final class PeriodicalFuzzyCheckpointTask implements Runnable {
    private PeriodicalFuzzyCheckpointTask() {
    }

    @Override
    public void run() {
      OLogSequenceNumber minLsn = writeAheadLog.getFlushedLSN();
      // if nothing is flushed yet, we do not need to proceed
      if (minLsn == null)
        return;

      for (Map.Entry<GroupKey, WriteGroup> entry : writeGroups.entrySet()) {
        Lock groupLock = lockManager.acquireExclusiveLock(entry.getKey());
        try {
          WriteGroup group = entry.getValue();
          for (int i = 0; i < 16; i++) {
            final OCachePointer pagePointer = group.pages[i];
            if (pagePointer != null && pagePointer.getLastFlushedLsn() != null) {
              if (minLsn.compareTo(pagePointer.getLastFlushedLsn()) > 0) {
                minLsn = pagePointer.getLastFlushedLsn();
              }
            }
          }
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }

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
    }
  }

  private final class FileFlushTask implements Callable<Void> {
    private final long fileId;

    private FileFlushTask(long fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final GroupKey firstKey = new GroupKey(fileId, 0);
      final GroupKey lastKey = new GroupKey(fileId, Long.MAX_VALUE);

      NavigableMap<GroupKey, WriteGroup> subMap = writeGroups.subMap(firstKey, true, lastKey, true);
      Iterator<Map.Entry<GroupKey, WriteGroup>> entryIterator = subMap.entrySet().iterator();

      groupsLoop: while (entryIterator.hasNext()) {
        Map.Entry<GroupKey, WriteGroup> entry = entryIterator.next();
        final WriteGroup writeGroup = entry.getValue();
        final GroupKey groupKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(groupKey);
        try {
          int flushedPages = 0;

          for (int i = 0; i < 16; i++) {
            OCachePointer pagePointer = writeGroup.pages[i];

            if (pagePointer != null) {
              if (!pagePointer.tryAcquireSharedLock())
                continue groupsLoop;

              try {
                flushPage(groupKey.fileId, (groupKey.groupIndex << 4) + i, pagePointer.getDataPointer());
                flushedPages++;
              } finally {
                pagePointer.releaseSharedLock();
              }
            }
          }

          for (OCachePointer pagePointer : writeGroup.pages)
            if (pagePointer != null)
              pagePointer.decrementReferrer();

          cacheSize.addAndGet(-flushedPages);
          entryIterator.remove();
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }

      files.get(fileId).synch();
      return null;
    }
  }

  private final class RemoveFilePagesTask implements Callable<Void> {
    private final long fileId;

    private RemoveFilePagesTask(long fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final GroupKey firstKey = new GroupKey(fileId, 0);
      final GroupKey lastKey = new GroupKey(fileId, Long.MAX_VALUE);

      NavigableMap<GroupKey, WriteGroup> subMap = writeGroups.subMap(firstKey, true, lastKey, true);
      Iterator<Map.Entry<GroupKey, WriteGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<GroupKey, WriteGroup> entry = entryIterator.next();
        WriteGroup writeGroup = entry.getValue();
        GroupKey groupKey = entry.getKey();

        Lock groupLock = lockManager.acquireExclusiveLock(groupKey);
        try {
          for (OCachePointer pagePointer : writeGroup.pages) {
            if (pagePointer != null) {
              pagePointer.acquireExclusiveLock();
              try {
                pagePointer.decrementReferrer();
                cacheSize.decrementAndGet();
              } finally {
                pagePointer.releaseExclusiveLock();
              }
            }
          }

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(groupLock);
        }
      }

      return null;
    }
  }

  private static class FlushThreadFactory implements ThreadFactory {
    private final String storageName;

    private FlushThreadFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
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
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("OrientDB Low Disk Space Publisher (" + storageName + ")");
      return thread;
    }
  }
}
