/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.concur.executors.SubScheduledExecutorService;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public class ODiskWriteAheadLog extends OAbstractWriteAheadLog {
  public static final  String MASTER_RECORD_EXTENSION = ".wmr";
  public static final  String WAL_SEGMENT_EXTENSION   = ".wal";
  private static final int    MASTER_RECORD_SIZE      = 20;
  private static final int    ONE_KB                  = 1024;
  private static final int    ONE_MB                  = ONE_KB * ONE_KB;

  private final long walSizeHardLimit = OGlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * ONE_KB * ONE_KB;
  private       long walSizeLimit     = walSizeHardLimit;

  private final    long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * ONE_KB * ONE_KB;
  private volatile long freeSpace      = -1;

  private volatile OLogSequenceNumber end;

  private final List<OLogSegment> logSegments = new ArrayList<>();
  private final int  maxPagesCacheSize;
  private final int  commitDelay;
  private final long maxSegmentSize;

  private final Path        walLocation;
  private final FileChannel masterRecordLSNHolder;

  /**
   * If file of {@link OLogSegment} will not be accessed inside of this interval (in seconds) it will be closed by timer.
   */
  private final int fileTTL;

  private final int segmentBufferSize;

  private final OLocalPaginatedStorage       storage;
  private final OPerformanceStatisticManager performanceStatisticManager;

  private boolean useFirstMasterRecord = true;
  private volatile long               logSize;
  private          Path               masterRecordPath;
  private          OLogSequenceNumber firstMasterRecord;
  private          OLogSequenceNumber secondMasterRecord;

  private final AtomicReference<OLogSequenceNumber> flushedLsn = new AtomicReference<>();

  private volatile OLogSequenceNumber preventCutTill;

  private volatile long cacheOverflowCount = 0;

  private       boolean   segmentCreationFlag     = false;
  private final Condition segmentCreationComplete = syncObject.newCondition();

  private final Set<OOperationUnitId>                           activeOperations        = new HashSet<>();
  private final List<WeakReference<OLowDiskSpaceListener>>      lowDiskSpaceListeners   = new CopyOnWriteArrayList<>();
  private final List<WeakReference<OCheckpointRequestListener>> fullCheckpointListeners = new CopyOnWriteArrayList<>();

  private final ScheduledThreadPoolExecutor autoFileCloser = new ScheduledThreadPoolExecutor(1, r -> {
    final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
    thread.setDaemon(true);
    thread.setName("WAL Closer Task (" + getStorage().getName() + ")");
    return thread;
  });

  private final ScheduledThreadPoolExecutor commitExecutor = new ScheduledThreadPoolExecutor(1, r -> {
    final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
    thread.setDaemon(true);
    thread.setName("OrientDB WAL Flush Task (" + getStorage().getName() + ")");
    return thread;
  });

  public ODiskWriteAheadLog(OLocalPaginatedStorage storage) throws IOException {
    this(storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.WAL_CACHE_SIZE),
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.WAL_COMMIT_TIMEOUT),
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE) * ONE_MB,
        storage.getConfiguration().getContextConfiguration().getValueAsString(OGlobalConfiguration.WAL_LOCATION), true, storage,
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.WAL_SEGMENT_BUFFER_SIZE)
            * ONE_MB,
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.WAL_FILE_AUTOCLOSE_INTERVAL));
  }

  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    lowDiskSpaceListeners.add(new WeakReference<>(listener));
  }

  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<>();

    for (WeakReference<OLowDiskSpaceListener> ref : lowDiskSpaceListeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OLowDiskSpaceListener> ref : itemsToRemove)
      lowDiskSpaceListeners.remove(ref);
  }

  public void addFullCheckpointListener(OCheckpointRequestListener listener) {
    fullCheckpointListeners.add(new WeakReference<>(listener));
  }

  public void removeFullCheckpointListener(OCheckpointRequestListener listener) {
    List<WeakReference<OCheckpointRequestListener>> itemsToRemove = new ArrayList<>();

    for (WeakReference<OCheckpointRequestListener> ref : fullCheckpointListeners) {
      final OCheckpointRequestListener fullCheckpointRequestListener = ref.get();

      if (fullCheckpointRequestListener == null || fullCheckpointRequestListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OCheckpointRequestListener> ref : itemsToRemove)
      fullCheckpointListeners.remove(ref);
  }

  public ODiskWriteAheadLog(int maxPagesCacheSize, int commitDelay, long maxSegmentSize, final String walPath,
      boolean filterWALFiles, final OLocalPaginatedStorage storage, int segmentBufferSize, int fileTTL) throws IOException {
    this.fileTTL = fileTTL;
    this.segmentBufferSize = segmentBufferSize;
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitDelay = commitDelay;
    this.maxSegmentSize = maxSegmentSize;
    this.storage = storage;
    this.performanceStatisticManager = storage.getPerformanceStatisticManager();

    try {
      this.walLocation = calculateWalPath(this.storage, walPath);

      Stream<Path> walFiles;

      final Locale locale = storage.getConfiguration().getLocaleInstance();
      final String storageName = storage.getName();

      if (filterWALFiles)
        walFiles = Files.find(walLocation, 1,
            (Path path, BasicFileAttributes attributes) -> validateName(path.getFileName().toString(), storageName, locale));
      else
        walFiles = Files.find(walLocation, 1,
            (Path path, BasicFileAttributes attrs) -> validateSimpleName(path.getFileName().toString(), locale));

      if (walFiles == null)
        throw new IllegalStateException(
            "Location passed in WAL does not exist, or IO error was happened. DB cannot work in durable mode in such case");

      logSize = 0;
      walFiles.forEach((Path path) -> {
        try {
          OLogSegment logSegment = new OLogSegment(this, path, maxPagesCacheSize, fileTTL, segmentBufferSize,
              new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
          logSegment.init();

          logSegments.add(logSegment);
          logSize += logSegment.filledUpTo();
        } catch (IOException e) {
          throw OException
              .wrapException(new OStorageException("Error during file initialization for storage '" + this.storage.getName() + "'"),
                  e);
        }
      });

      if (logSegments.isEmpty()) {
        OLogSegment logSegment = new OLogSegment(this, this.walLocation.resolve(getSegmentName(0)), maxPagesCacheSize, fileTTL,
            segmentBufferSize, new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
        logSegment.init();
        logSegment.startFlush();
        logSegments.add(logSegment);

        flushedLsn.set(null);
      } else {
        Collections.sort(logSegments);

        logSegments.get(logSegments.size() - 1).startFlush();

        flushedLsn.set(readFlushedLSN());

        end = calculateEndLSN();
      }

      masterRecordPath = walLocation.resolve(this.storage.getName() + MASTER_RECORD_EXTENSION);
      masterRecordLSNHolder = FileChannel
          .open(masterRecordPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE,
              StandardOpenOption.SYNC);

      if (masterRecordLSNHolder.size() > 0) {
        firstMasterRecord = readMasterRecord(this.storage.getName(), 0);
        secondMasterRecord = readMasterRecord(this.storage.getName(), 1);

        if (firstMasterRecord == null) {
          useFirstMasterRecord = true;
          lastCheckpoint = secondMasterRecord;
        } else if (secondMasterRecord == null) {
          useFirstMasterRecord = false;
          lastCheckpoint = firstMasterRecord;
        } else {
          if (firstMasterRecord.compareTo(secondMasterRecord) >= 0) {
            lastCheckpoint = firstMasterRecord;
            useFirstMasterRecord = false;
          } else {
            lastCheckpoint = secondMasterRecord;
            useFirstMasterRecord = true;
          }
        }
      }

      fixMasterRecords();

    } catch (FileNotFoundException e) {
      // never happened
      OLogManager.instance().error(this, "Error during file initialization for storage '%s'", e, this.storage.getName());
      throw new IllegalStateException("Error during file initialization for storage '" + this.storage.getName() + "'", e);
    }
  }

  private OLogSequenceNumber calculateEndLSN() {
    int lastIndex = logSegments.size() - 1;
    OLogSegment last = logSegments.get(lastIndex);

    while (last.getFilledUpTo() == 0) {
      lastIndex--;
      if (lastIndex >= 0)
        last = logSegments.get(lastIndex);
      else
        return null;
    }

    return last.end();
  }

  void incrementCacheOverflowCount() {
    cacheOverflowCount++;
  }

  public long getCacheOverflowCount() {
    return cacheOverflowCount;
  }

  private Path calculateWalPath(OLocalPaginatedStorage storage, String walPath) {
    if (walPath == null)
      return storage.getStoragePath();

    return Paths.get(walPath);
  }

  private String getSegmentName(long order) {
    return storage.getName() + "." + order + WAL_SEGMENT_EXTENSION;
  }

  private static boolean validateSimpleName(String name, Locale locale) {
    if (!name.toLowerCase(locale).endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      //noinspection ResultOfMethodCallIgnored
      Integer.parseInt(walOrder);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  private static boolean validateName(String name, String storageName, Locale locale) {
    if (!name.toLowerCase(locale).endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final String walStorageName = name.substring(0, walOrderStartIndex);
    if (!storageName.equals(walStorageName))
      return false;

    int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      //noinspection ResultOfMethodCallIgnored
      Integer.parseInt(walOrder);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  Path getWalLocation() {
    return walLocation;
  }

  @Override
  public OLogSequenceNumber begin() throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      OLogSegment first = logSegments.get(0);
      if (first.filledUpTo() == 0)
        return null;

      return first.begin();

    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber begin(long segmentId) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      for (OLogSegment logSegment : logSegments) {
        if (logSegment.getOrder() == segmentId) {
          return logSegment.begin();
        }
      }

    } finally {
      syncObject.unlock();
    }

    return null;
  }

  public OLogSequenceNumber end() {
    return end;
  }

  public void flush() throws IOException {
    OLogSegment last;

    syncObject.lock();
    try {
      checkForClose();

      last = logSegments.get(logSegments.size() - 1);
    } finally {
      syncObject.unlock();
    }

    last.flush();
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) throws IOException {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();

    if (statistic != null)
      statistic.startWALLogRecordTimer();
    try {
      OAtomicUnitStartRecord record = new OAtomicUnitStartRecord(isRollbackSupported, unitId);
      byte[] content = OWALRecordsFactory.INSTANCE.toStream(record);
      syncObject.lock();
      try {
        checkForClose();

        final OLogSequenceNumber lsn = internalLog(record, content);
        activeOperations.add(unitId);
        return lsn;
      } finally {
        syncObject.unlock();
      }
    } finally {
      if (statistic != null)
        statistic.stopWALRecordTimer(true, false);
    }
  }

  @Override
  public OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback,
      OLogSequenceNumber startLsn, Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) throws IOException {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();

    if (statistic != null)
      statistic.startWALLogRecordTimer();
    try {
      OAtomicUnitEndRecord record = new OAtomicUnitEndRecord(operationUnitId, rollback, atomicOperationMetadata);
      byte[] content = OWALRecordsFactory.INSTANCE.toStream(record);
      syncObject.lock();
      try {
        checkForClose();

        final OLogSequenceNumber lsn = internalLog(record, content);
        activeOperations.remove(operationUnitId);

        return lsn;
      } finally {
        syncObject.unlock();
      }
    } finally {
      if (statistic != null)
        statistic.stopWALRecordTimer(false, true);
    }
  }

  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null)
      statistic.startWALLogRecordTimer();
    try {
      return internalLog(record, OWALRecordsFactory.INSTANCE.toStream(record));
    } finally {
      if (statistic != null)
        statistic.stopWALRecordTimer(false, false);
    }

  }

  /**
   * it log a record getting the serialized content as parameter.
   */
  private OLogSequenceNumber internalLog(OWALRecord record, byte[] recordContent) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      if (segmentCreationFlag && record instanceof OOperationUnitRecord && !activeOperations
          .contains(((OOperationUnitRecord) record).getOperationUnitId())) {
        while (segmentCreationFlag) {
          try {
            segmentCreationComplete.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OInterruptedException("Segment creation was interrupted");
          }
        }
      }

      OLogSegment last = logSegments.get(logSegments.size() - 1);
      long lastSize = last.filledUpTo();

      final OLogSequenceNumber lsn = last.logRecord(recordContent);
      record.setLsn(lsn);

      end = lsn;

      if (record.isUpdateMasterRecord()) {
        lastCheckpoint = lsn;
        if (useFirstMasterRecord) {
          firstMasterRecord = lsn;
          writeMasterRecord(0, firstMasterRecord);
          useFirstMasterRecord = false;
        } else {
          secondMasterRecord = lsn;
          writeMasterRecord(1, secondMasterRecord);
          useFirstMasterRecord = true;
        }
      }

      final long sizeDiff = last.filledUpTo() - lastSize;
      logSize += sizeDiff;

      if (last.filledUpTo() >= maxSegmentSize) {
        segmentCreationFlag = true;

        if (record instanceof OAtomicUnitEndRecord && activeOperations.size() == 1 || (!(record instanceof OOperationUnitRecord)
            && activeOperations.isEmpty())) {
          last.stopFlush(true);

          last = new OLogSegment(this, walLocation.resolve(getSegmentName(last.getOrder() + 1)), maxPagesCacheSize, fileTTL,
              segmentBufferSize, new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
          last.init();
          last.startFlush();

          logSegments.add(last);

          segmentCreationFlag = false;
          segmentCreationComplete.signalAll();
        }
      }

      if (walSizeHardLimit < 0 && freeSpace > -1) {
        walSizeLimit += (logSize + freeSpace) / 2;
      }

      if (walSizeLimit > -1 && logSize > walSizeLimit && logSegments.size() > 1) {
        for (WeakReference<OCheckpointRequestListener> listenerWeakReference : fullCheckpointListeners) {
          final OCheckpointRequestListener listener = listenerWeakReference.get();
          if (listener != null)
            listener.requestCheckpoint();
        }
      }

      return lsn;

    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void moveLsnAfter(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      if (!activeOperations.isEmpty())
        throw new OStorageException("Can not change end of WAL because there are active atomic operations in the log.");

      if (end() == null)
        throw new OStorageException("Can not change end of WAL because WAL is empty");

      if (end().compareTo(lsn) > 0)
        return;

      OLogSegment last = logSegments.get(logSegments.size() - 1);
      last.stopFlush(true);

      if (last.filledUpTo() == 0) {
        last.delete(false);
        logSegments.remove(logSegments.size() - 1);
      }

      last = new OLogSegment(this, walLocation.resolve(getSegmentName(lsn.getSegment() + 1)), maxPagesCacheSize, fileTTL,
          segmentBufferSize, new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
      last.init();
      last.startFlush();

      logSegments.add(last);

    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void newSegment() throws IOException {
    syncObject.lock();
    try {
      if (!activeOperations.isEmpty())
        throw new OStorageException("Can not change end of WAL because there are active atomic operations in the log.");

      OLogSegment last = logSegments.get(logSegments.size() - 1);
      if (last.filledUpTo() == 0) {
        return;
      }

      last.stopFlush(true);

      last = new OLogSegment(this, walLocation.resolve(getSegmentName(last.getOrder() + 1)), maxPagesCacheSize, fileTTL,
          segmentBufferSize, new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
      last.init();
      last.startFlush();

      logSegments.add(last);
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public long activeSegment() {
    syncObject.lock();
    try {
      final OLogSegment last = logSegments.get(logSegments.size() - 1);
      return last.getOrder();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public File[] nonActiveSegments(long fromSegment) {
    final List<File> result = new ArrayList<>();

    syncObject.lock();
    try {
      for (int i = 0; i < logSegments.size() - 1; i++) {
        final OLogSegment logSegment = logSegments.get(i);

        if (logSegment.getOrder() >= fromSegment) {
          final File fileLog = logSegment.getPath().toFile();
          result.add(fileLog);
        }
      }
    } finally {
      syncObject.unlock();
    }

    File[] files = new File[result.size()];
    files = result.toArray(files);

    return files;
  }

  @Override
  public long[] nonActiveSegments() {
    final long[] result;

    syncObject.lock();
    try {
      result = new long[logSegments.size() - 1];

      for (int i = 0; i < logSegments.size() - 1; i++) {
        final OLogSegment logSegment = logSegments.get(i);
        result[i] = logSegment.getOrder();

      }
    } finally {
      syncObject.unlock();
    }

    return result;
  }

  public long size() {
    return logSize;
  }

  public List<String> getWalFiles() {
    final ArrayList<String> result = new ArrayList<>();
    syncObject.lock();
    try {
      for (OLogSegment segment : logSegments) {
        result.add(segment.getPath().toString());
      }
    } finally {
      syncObject.unlock();
    }

    return result;
  }

  public Path getWMRFile() {
    syncObject.lock();
    try {
      return masterRecordPath;
    } finally {
      syncObject.unlock();
    }
  }

  public void truncate() throws IOException {
    syncObject.lock();
    try {
      if (logSegments.size() < 2)
        return;

      ListIterator<OLogSegment> iterator = logSegments.listIterator(logSegments.size() - 1);
      while (iterator.hasPrevious()) {
        final OLogSegment logSegment = iterator.previous();
        logSegment.delete(false);
        iterator.remove();
      }

      recalculateLogSize();
    } finally {
      syncObject.unlock();
    }
  }

  public void close() throws IOException {
    close(true);
  }

  public void close(boolean flush) throws IOException {
    syncObject.lock();
    try {
      if (closed)
        return;

      closed = true;

      for (OLogSegment logSegment : logSegments)
        logSegment.close(flush);

      if (!commitExecutor.isShutdown()) {
        commitExecutor.shutdown();
        try {
          if (!commitExecutor
              .awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
            throw new OStorageException("WAL flush task for '" + getStorage().getName() + "' storage cannot be stopped");

        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread");
        }
      }

      if (!autoFileCloser.isShutdown()) {
        autoFileCloser.shutdown();
        try {
          if (!autoFileCloser
              .awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
            throw new OStorageException("WAL file auto close tasks '" + getStorage().getName() + "' storage cannot be stopped");

        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Shutdown of file auto close tasks was interrupted");
        }
      }

      masterRecordLSNHolder.close();
    } finally {
      syncObject.unlock();
    }
  }

  public void delete() throws IOException {
    delete(false);
  }

  public void delete(boolean flush) throws IOException {
    syncObject.lock();
    try {
      close(flush);

      for (OLogSegment logSegment : logSegments)
        logSegment.delete(false);

      Files.deleteIfExists(masterRecordPath);
    } finally {
      syncObject.unlock();
    }
  }

  public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      long segment = lsn.getSegment();
      int index = (int) (segment - logSegments.get(0).getOrder());

      if (index < 0 || index >= logSegments.size())
        return null;

      OLogSegment logSegment = logSegments.get(index);
      byte[] recordEntry = logSegment.readRecord(lsn);
      if (recordEntry == null)
        return null;

      final OWALRecord record = OWALRecordsFactory.INSTANCE.fromStream(recordEntry);
      record.setLsn(lsn);

      return record;

    } finally {
      syncObject.unlock();
    }
  }

  public OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      long order = lsn.getSegment();
      int index = (int) (order - logSegments.get(0).getOrder());

      if (index < 0 || index >= logSegments.size())
        return null;

      OLogSegment logSegment = logSegments.get(index);
      OLogSequenceNumber nextLSN = logSegment.getNextLSN(lsn);

      if (nextLSN == null) {
        index++;
        if (index >= logSegments.size())
          return null;

        OLogSegment nextSegment = logSegments.get(index);
        if (nextSegment.filledUpTo() == 0)
          return null;

        nextLSN = nextSegment.begin();
      }

      return nextLSN;
    } finally {
      syncObject.unlock();
    }
  }

  public OLogSequenceNumber getFlushedLsn() {
    return flushedLsn.get();
  }

  public void cutTill(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      flush();

      final OLogSequenceNumber maxLsn = preventCutTill;

      if (maxLsn != null && lsn.compareTo(maxLsn) > 0)
        lsn = maxLsn;

      int lastTruncateIndex = -1;

      for (int i = 0; i < logSegments.size() - 1; i++) {
        final OLogSegment logSegment = logSegments.get(i);

        if (logSegment.end().compareTo(lsn) < 0)
          lastTruncateIndex = i;
        else
          break;
      }

      for (int i = 0; i <= lastTruncateIndex; i++) {
        final OLogSegment logSegment = removeHeadSegmentFromList();
        if (logSegment != null)
          logSegment.delete(false);
      }

      recalculateLogSize();
      fixMasterRecords();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void cutAllSegmentsSmallerThan(long segmentId) throws IOException {
    syncObject.lock();
    try {
      checkForClose();
      flush();

      final OLogSequenceNumber maxSegmentLSN = preventCutTill;

      if (maxSegmentLSN != null) {
        if (segmentId > maxSegmentLSN.getSegment()) {
          segmentId = maxSegmentLSN.getSegment();
        }
      }

      int lastTruncateIndex = -1;

      for (int i = 0; i < logSegments.size() - 1; i++) {
        final OLogSegment logSegment = logSegments.get(i);

        if (logSegment.getOrder() < segmentId)
          lastTruncateIndex = i;
        else
          break;
      }

      for (int i = 0; i <= lastTruncateIndex; i++) {
        final OLogSegment logSegment = removeHeadSegmentFromList();

        if (logSegment != null)
          logSegment.delete(false);
      }

      recalculateLogSize();
      fixMasterRecords();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void preventCutTill(OLogSequenceNumber lsn) throws IOException {
    preventCutTill = lsn;
  }

  private OLogSegment removeHeadSegmentFromList() {
    if (logSegments.size() < 2)
      return null;

    return logSegments.remove(0);
  }

  private void recalculateLogSize() throws IOException {
    logSize = 0;

    for (OLogSegment segment : logSegments)
      logSize += segment.filledUpTo();
  }

  private void fixMasterRecords() throws IOException {
    if (firstMasterRecord != null) {
      int index = (int) (firstMasterRecord.getSegment() - logSegments.get(0).getOrder());
      if (logSegments.size() <= index || index < 0) {
        firstMasterRecord = null;
      } else {
        OLogSegment firstMasterRecordSegment = logSegments.get(index);
        if (firstMasterRecordSegment.filledUpTo() <= firstMasterRecord.getPosition())
          firstMasterRecord = null;
      }
    }

    if (secondMasterRecord != null) {
      int index = (int) (secondMasterRecord.getSegment() - logSegments.get(0).getOrder());
      if (logSegments.size() <= index || index < 0) {
        secondMasterRecord = null;
      } else {
        OLogSegment secondMasterRecordSegment = logSegments.get(index);
        if (secondMasterRecordSegment.filledUpTo() <= secondMasterRecord.getPosition())
          secondMasterRecord = null;
      }
    }

    if (firstMasterRecord != null && secondMasterRecord != null)
      return;

    if (firstMasterRecord == null && secondMasterRecord == null) {
      masterRecordLSNHolder.truncate(0);
      masterRecordLSNHolder.force(true);
      lastCheckpoint = null;
    } else {
      if (secondMasterRecord == null)
        secondMasterRecord = firstMasterRecord;
      else
        firstMasterRecord = secondMasterRecord;

      lastCheckpoint = firstMasterRecord;

      writeMasterRecord(0, firstMasterRecord);
      writeMasterRecord(1, secondMasterRecord);
    }
  }

  private OLogSequenceNumber readMasterRecord(String storageName, int index) throws IOException {
    final long masterPosition = index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE);

    if (masterRecordLSNHolder.size() < masterPosition + MASTER_RECORD_SIZE) {
      OLogManager.instance().debug(this, "Cannot restore %d WAL master record for storage %s", index, storageName);
      return null;
    }

    final CRC32 crc32 = new CRC32();
    try {
      ByteBuffer buffer = ByteBuffer.allocate(MASTER_RECORD_SIZE);

      OIOUtils.readByteBuffer(buffer, masterRecordLSNHolder, masterPosition);
      buffer.rewind();

      int firstCRC = buffer.getInt();
      final long segment = buffer.getLong();
      final long position = buffer.getLong();

      byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serializeLiteral(segment, serializedLSN, 0);
      OLongSerializer.INSTANCE.serializeLiteral(position, serializedLSN, OLongSerializer.LONG_SIZE);
      crc32.update(serializedLSN);

      if (firstCRC != ((int) crc32.getValue())) {
        OLogManager.instance()
            .error(this, "Cannot restore %d WAL master record for storage %s crc check is failed", index, storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (EOFException eofException) {
      OLogManager.instance().debug(this, "Cannot restore %d WAL master record for storage %s", index, storageName);
      return null;
    }
  }

  private void writeMasterRecord(int index, OLogSequenceNumber masterRecord) throws IOException {
    masterRecordLSNHolder.position();
    final CRC32 crc32 = new CRC32();

    final byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getPosition(), serializedLSN, OLongSerializer.LONG_SIZE);
    crc32.update(serializedLSN);

    ByteBuffer buffer = ByteBuffer.allocate(MASTER_RECORD_SIZE);

    buffer.putInt((int) crc32.getValue());
    buffer.putLong(masterRecord.getSegment());
    buffer.putLong(masterRecord.getPosition());
    buffer.rewind();

    OIOUtils.writeByteBuffer(buffer, masterRecordLSNHolder, index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE));
  }

  private OLogSequenceNumber readFlushedLSN() throws IOException {
    int segment = logSegments.size() - 1;
    while (segment >= 0) {
      OLogSegment logSegment = logSegments.get(segment);
      OLogSequenceNumber flushedLSN = logSegment.readFlushedLSN();
      if (flushedLSN == null)
        segment--;
      else
        return flushedLSN;
    }

    return null;
  }

  public OLocalPaginatedStorage getStorage() {
    return storage;
  }

  void casFlushedLsn(OLogSequenceNumber flushedLsn) {
    if (flushedLsn == null)
      return;

    OLogSequenceNumber lsn = this.flushedLsn.get();
    while (lsn == null || flushedLsn.compareTo(lsn) > 0) {
      if (this.flushedLsn.compareAndSet(lsn, flushedLsn))
        return;

      lsn = this.flushedLsn.get();
    }
  }

  public void checkFreeSpace() throws IOException {
    freeSpace = Files.getFileStore(walLocation).getUsableSpace();

    //system has unlimited amount of free space
    if (freeSpace < 0)
      return;

    if (freeSpace < freeSpaceLimit) {
      for (WeakReference<OLowDiskSpaceListener> listenerWeakReference : lowDiskSpaceListeners) {
        final OLowDiskSpaceListener lowDiskSpaceListener = listenerWeakReference.get();

        if (lowDiskSpaceListener != null)
          lowDiskSpaceListener.lowDiskSpace(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));
      }
    }
  }

  int getCommitDelay() {
    return commitDelay;
  }

}
