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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.concur.executors.SubScheduledExecutorService;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OFullCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class ODiskWriteAheadLog extends OAbstractWriteAheadLog {
  public static final  String MASTER_RECORD_EXTENSION = ".wmr";
  public static final  String WAL_SEGMENT_EXTENSION   = ".wal";
  private static final long   ONE_KB                  = 1024L;

  private final long freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * ONE_KB * ONE_KB;
  private final long walSizeLimit   = OGlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * ONE_KB * ONE_KB;

  private final List<OLogSegment> logSegments = new ArrayList<OLogSegment>();
  private final int              maxPagesCacheSize;
  private final int              commitDelay;
  private final long             maxSegmentSize;
  private final long             preferredSegmentCount;
  private final File             walLocation;
  private final RandomAccessFile masterRecordLSNHolder;

  /**
   * If file of {@link OLogSegment} will not be accessed inside of this interval (in seconds) it will be closed by timer.
   */
  private final int fileTTL;

  private final OLocalPaginatedStorage       storage;
  private final OPerformanceStatisticManager performanceStatisticManager;

  private boolean useFirstMasterRecord = true;
  private volatile long               logSize;
  private          File               masterRecordFile;
  private          OLogSequenceNumber firstMasterRecord;
  private          OLogSequenceNumber secondMasterRecord;

  private volatile OLogSequenceNumber flushedLsn;

  private volatile OLogSequenceNumber preventCutTill;

  private volatile long cacheOverflowCount = 0;

  private       boolean   segmentCreationFlag     = false;
  private final Condition segmentCreationComplete = syncObject.newCondition();

  private final Set<OOperationUnitId>                               activeOperations        = new HashSet<OOperationUnitId>();
  private final List<WeakReference<OLowDiskSpaceListener>>          lowDiskSpaceListeners   = new CopyOnWriteArrayList<WeakReference<OLowDiskSpaceListener>>();
  private final List<WeakReference<OFullCheckpointRequestListener>> fullCheckpointListeners = new CopyOnWriteArrayList<WeakReference<OFullCheckpointRequestListener>>();

  private final ByteBuffer fileDataBuffer = ByteBuffer.allocateDirect(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

  private static class SimpleFileNameFilter implements java.io.FilenameFilter {
    private final String storageName;
    private final Locale locale;

    public SimpleFileNameFilter(OLocalPaginatedStorage storage) {
      this.storageName = storage.getName();
      this.locale = storage.getConfiguration().getLocaleInstance();
    }

    @Override
    public boolean accept(File dir, String name) {
      return validateSimpleName(name, storageName, locale);
    }
  }

  private final ScheduledThreadPoolExecutor autoFileCloser = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
    @SuppressWarnings("NullableProblems")
    @Override
    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("WAL Closer Task (" + getStorage().getName() + ")");
      return thread;
    }
  });

  private final ScheduledThreadPoolExecutor commitExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
    @SuppressWarnings("NullableProblems")
    @Override
    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task (" + getStorage().getName() + ")");
      return thread;
    }
  });

  private static class FilenameFilter implements java.io.FilenameFilter {
    private final String storageName;
    private final Locale locale;

    FilenameFilter(OLocalPaginatedStorage storage) {
      this.storageName = storage.getName();
      this.locale = storage.getConfiguration().getLocaleInstance();
    }

    @Override
    public boolean accept(File dir, String name) {
      return validateName(name, storageName, locale);
    }
  }

  public ODiskWriteAheadLog(OLocalPaginatedStorage storage) throws IOException {
    this(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(), OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger() * ONE_KB * ONE_KB,
        OGlobalConfiguration.WAL_LOCATION.getValueAsString(), true, storage,
        OGlobalConfiguration.WAL_FILE_AUTOCLOSE_INTERVAL.getValueAsInteger());
  }

  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    lowDiskSpaceListeners.add(new WeakReference<OLowDiskSpaceListener>(listener));
  }

  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    List<WeakReference<OLowDiskSpaceListener>> itemsToRemove = new ArrayList<WeakReference<OLowDiskSpaceListener>>();

    for (WeakReference<OLowDiskSpaceListener> ref : lowDiskSpaceListeners) {
      final OLowDiskSpaceListener lowDiskSpaceListener = ref.get();

      if (lowDiskSpaceListener == null || lowDiskSpaceListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OLowDiskSpaceListener> ref : itemsToRemove)
      lowDiskSpaceListeners.remove(ref);
  }

  public void addFullCheckpointListener(OFullCheckpointRequestListener listener) {
    fullCheckpointListeners.add(new WeakReference<OFullCheckpointRequestListener>(listener));
  }

  public void removeFullCheckpointListener(OFullCheckpointRequestListener listener) {
    List<WeakReference<OFullCheckpointRequestListener>> itemsToRemove = new ArrayList<WeakReference<OFullCheckpointRequestListener>>();

    for (WeakReference<OFullCheckpointRequestListener> ref : fullCheckpointListeners) {
      final OFullCheckpointRequestListener fullCheckpointRequestListener = ref.get();

      if (fullCheckpointRequestListener == null || fullCheckpointRequestListener.equals(listener))
        itemsToRemove.add(ref);
    }

    for (WeakReference<OFullCheckpointRequestListener> ref : itemsToRemove)
      fullCheckpointListeners.remove(ref);
  }

  /**
   * @param fileTTL If file of {@link OLogSegment} will not be accessed inside of this interval (in seconds) it will be closed by
   *                timer.
   */
  public ODiskWriteAheadLog(int maxPagesCacheSize, int commitDelay, long maxSegmentSize, final String walPath,
      boolean filterWALFiles, final OLocalPaginatedStorage storage, int fileTTL) throws IOException {
    this.fileTTL = fileTTL;
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitDelay = commitDelay;
    this.maxSegmentSize = maxSegmentSize;
    this.preferredSegmentCount = walSizeLimit / maxSegmentSize;
    this.storage = storage;
    this.performanceStatisticManager = storage.getPerformanceStatisticManager();

    try {
      this.walLocation = new File(calculateWalPath(this.storage, walPath));

      File[] walFiles;
      if (filterWALFiles)
        walFiles = this.walLocation.listFiles(new FilenameFilter(storage));
      else
        walFiles = this.walLocation.listFiles(new SimpleFileNameFilter(storage));

      if (walFiles == null)
        throw new IllegalStateException(
            "Location passed in WAL does not exist, or IO error was happened. DB cannot work in durable mode in such case");

      if (walFiles.length == 0) {
        OLogSegment logSegment = new OLogSegment(this, new File(this.walLocation, getSegmentName(0)), fileTTL, maxPagesCacheSize,
            performanceStatisticManager, new SubScheduledExecutorService(autoFileCloser),
            new SubScheduledExecutorService(commitExecutor));

        logSegment.init(fileDataBuffer);
        logSegment.startFlush();
        logSegments.add(logSegment);

        logSize = 0;

        flushedLsn = null;
      } else {

        logSize = 0;

        for (File walFile : walFiles) {
          OLogSegment logSegment = new OLogSegment(this, walFile, fileTTL, maxPagesCacheSize, performanceStatisticManager,
              new SubScheduledExecutorService(autoFileCloser), new SubScheduledExecutorService(commitExecutor));
          logSegment.init(fileDataBuffer);

          logSegments.add(logSegment);
          logSize += logSegment.filledUpTo();
        }

        Collections.sort(logSegments);

        logSegments.get(logSegments.size() - 1).startFlush();
        flushedLsn = readFlushedLSN();
      }

      masterRecordFile = new File(walLocation, this.storage.getName() + MASTER_RECORD_EXTENSION);
      masterRecordLSNHolder = new RandomAccessFile(masterRecordFile, "rws");

      if (masterRecordLSNHolder.length() > 0) {
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

  public void incrementCacheOverflowCount() {
    cacheOverflowCount++;
  }

  public long getCacheOverflowCount() {
    return cacheOverflowCount;
  }

  private String calculateWalPath(OLocalPaginatedStorage storage, String walPath) {
    if (walPath == null)
      return storage.getStoragePath();

    return walPath;
  }

  private String getSegmentName(long order) {
    return storage.getName() + "." + order + WAL_SEGMENT_EXTENSION;
  }

  static boolean validateName(String name, String storageName, Locale locale) {
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
      Integer.parseInt(walOrder);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  static boolean validateSimpleName(String name, String storageName, Locale locale) {
    if (!name.toLowerCase(locale).endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      Integer.parseInt(walOrder);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  public File getWalLocation() {
    return walLocation;
  }

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

  public OLogSequenceNumber end() {
    syncObject.lock();
    try {
      checkForClose();

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
    } finally {
      syncObject.unlock();
    }
  }

  public void flush() {
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
   * it log a record getting the serialized content as paramenter.
   *
   * @param record
   * @param recordContent
   *
   * @return
   *
   * @throws IOException
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

          last = new OLogSegment(this, new File(walLocation, getSegmentName(last.getOrder() + 1)), fileTTL, maxPagesCacheSize,
              performanceStatisticManager, new SubScheduledExecutorService(autoFileCloser),
              new SubScheduledExecutorService(commitExecutor));
          last.init(fileDataBuffer);
          last.startFlush();

          logSegments.add(last);

          segmentCreationFlag = false;
          segmentCreationComplete.signalAll();
        }
      }

      if (logSize > walSizeLimit && logSegments.size() > 1) {
        for (WeakReference<OFullCheckpointRequestListener> listenerWeakReference : fullCheckpointListeners) {
          final OFullCheckpointRequestListener listener = listenerWeakReference.get();
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

      last = new OLogSegment(this, new File(walLocation, getSegmentName(lsn.getSegment() + 1)), fileTTL, maxPagesCacheSize,
          performanceStatisticManager, new SubScheduledExecutorService(autoFileCloser),
          new SubScheduledExecutorService(commitExecutor));
      last.init(fileDataBuffer);
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

      last = new OLogSegment(this, new File(walLocation, getSegmentName(last.getOrder() + 1)), fileTTL, maxPagesCacheSize,
          performanceStatisticManager, new SubScheduledExecutorService(autoFileCloser),
          new SubScheduledExecutorService(commitExecutor));
      last.init(fileDataBuffer);
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
    final List<File> result = new ArrayList<File>();

    syncObject.lock();
    try {
      for (int i = 0; i < logSegments.size() - 1; i++) {
        final OLogSegment logSegment = logSegments.get(i);

        if (logSegment.getOrder() >= fromSegment) {
          final File fileLog = new File(logSegment.getPath());
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

  public long size() {
    return logSize;
  }

  public List<String> getWalFiles() {
    final ArrayList<String> result = new ArrayList<String>();
    syncObject.lock();
    try {
      for (OLogSegment segment : logSegments) {
        result.add(segment.getPath());
      }
    } finally {
      syncObject.unlock();
    }

    return result;
  }

  public String getWMRFile() {
    syncObject.lock();
    try {
      return masterRecordFile.getAbsolutePath();
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

      boolean deleted = OFileUtils.delete(masterRecordFile);
      int retryCount = 0;

      while (!deleted) {
        deleted = OFileUtils.delete(masterRecordFile);
        retryCount++;

        if (retryCount > 10)
          throw new IOException("Cannot delete file. Retry limit exceeded. (" + retryCount + ")");
      }
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
      byte[] recordEntry = logSegment.readRecord(lsn, fileDataBuffer);
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
      OLogSequenceNumber nextLSN = logSegment.getNextLSN(lsn, fileDataBuffer);

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
    return flushedLsn;
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
  public void preventCutTill(OLogSequenceNumber lsn) throws IOException {
    preventCutTill = lsn;
  }

  @Override
  public long getPreferredSegmentCount() {
    return preferredSegmentCount;
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
      masterRecordLSNHolder.setLength(0);
      masterRecordLSNHolder.getFD().sync();
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
    final CRC32 crc32 = new CRC32();
    try {
      masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE));

      int firstCRC = masterRecordLSNHolder.readInt();
      final long segment = masterRecordLSNHolder.readLong();
      final long position = masterRecordLSNHolder.readLong();

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
    masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE));
    final CRC32 crc32 = new CRC32();

    final byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getPosition(), serializedLSN, OLongSerializer.LONG_SIZE);
    crc32.update(serializedLSN);

    byte[] record = new byte[OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE];

    OIntegerSerializer.INSTANCE.serializeLiteral((int) crc32.getValue(), record, 0);
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getSegment(), record, OIntegerSerializer.INT_SIZE);
    OLongSerializer.INSTANCE
        .serializeLiteral(masterRecord.getPosition(), record, OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
    masterRecordLSNHolder.write(record);
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

  public void setFlushedLsn(OLogSequenceNumber flushedLsn) {
    this.flushedLsn = flushedLsn;
  }


  public void checkFreeSpace() {
    final long freeSpace = walLocation.getFreeSpace();
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

  public int getCommitDelay() {
    return commitDelay;
  }

}
