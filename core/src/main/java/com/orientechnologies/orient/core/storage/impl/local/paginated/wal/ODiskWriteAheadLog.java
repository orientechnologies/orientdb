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

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OFullCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class ODiskWriteAheadLog extends OAbstractWriteAheadLog {
  public static final String                                        MASTER_RECORD_EXTENSION = ".wmr";
  public static final String                                        WAL_SEGMENT_EXTENSION   = ".wal";
  private static final long                                         ONE_KB                  = 1024L;

  private final long                                                freeSpaceLimit          = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT
                                                                                                .getValueAsLong() * 1024L * 1024L;
  private final long                                                walSizeLimit            = OGlobalConfiguration.WAL_MAX_SIZE
                                                                                                .getValueAsLong() * 1024L * 1024L;

  private final List<LogSegment>                                    logSegments             = new ArrayList<LogSegment>();
  private final int                                                 maxPagesCacheSize;
  private final int                                                 commitDelay;
  private final long                                                maxSegmentSize;
  private final File                                                walLocation;
  private final RandomAccessFile                                    masterRecordLSNHolder;
  private final OLocalPaginatedStorage                              storage;
  private boolean                                                   useFirstMasterRecord    = true;
  private long                                                      logSize;
  private File                                                      masterRecordFile;
  private OLogSequenceNumber                                        firstMasterRecord;
  private OLogSequenceNumber                                        secondMasterRecord;
  private volatile OLogSequenceNumber                               flushedLsn;

  private boolean                                                   segmentCreationFlag     = false;
  private final Condition                                           segmentCreationComplete = syncObject.newCondition();

  private final Set<OOperationUnitId>                               activeOperations        = new HashSet<OOperationUnitId>();
  private final List<WeakReference<OLowDiskSpaceListener>>          lowDiskSpaceListeners   = Collections
                                                                                                .synchronizedList(new ArrayList<WeakReference<OLowDiskSpaceListener>>());
  private final List<WeakReference<OFullCheckpointRequestListener>> fullCheckpointListeners = Collections
                                                                                                .synchronizedList(new ArrayList<WeakReference<OFullCheckpointRequestListener>>());

  private final class LogSegment implements Comparable<LogSegment> {
    private final RandomAccessFile                           rndFile;
    private final File                                       file;
    private final long                                       order;
    private final int                                        maxPagesCacheSize;
    private final ConcurrentLinkedQueue<OWALPage>            pagesCache     = new ConcurrentLinkedQueue<OWALPage>();
    private final ScheduledExecutorService                   commitExecutor = Executors
                                                                                .newSingleThreadScheduledExecutor(new ThreadFactory() {
                                                                                  @Override
                                                                                  public Thread newThread(Runnable r) {
                                                                                    final Thread thread = new Thread(
                                                                                        OStorageAbstract.storageThreadGroup, r);
                                                                                    thread.setDaemon(true);
                                                                                    thread.setName("OrientDB WAL Flush Task ("
                                                                                        + storage.getName() + ")");
                                                                                    return thread;
                                                                                  }
                                                                                });
    private long                                             filledUpTo;
    private boolean                                          closed;
    private OWALPage                                         currentPage;
    private long                                             nextPositionToFlush;
    private OLogSequenceNumber                               last           = null;
    private OLogSequenceNumber                               pendingLSNToFlush;

    private volatile boolean                                 flushNewData   = true;

    private WeakReference<OPair<OLogSequenceNumber, byte[]>> lastReadRecord = new WeakReference<OPair<OLogSequenceNumber, byte[]>>(
                                                                                null);

    private final class FlushTask implements Runnable {
      private FlushTask() {
      }

      @Override
      public void run() {
        try {
          commit();
        } catch (Throwable e) {
          OLogManager.instance().error(this, "Error during WAL background flush", e);
        }
      }

      private void commit() throws IOException {
        if (pagesCache.isEmpty())
          return;

        if (!flushNewData)
          return;

        flushNewData = false;

        final int maxSize = pagesCache.size();

        ODirectMemoryPointer[] pagesToFlush = new ODirectMemoryPointer[maxSize];

        long filePointer = nextPositionToFlush;

        int flushedPages = 0;
        OLogSequenceNumber lastLSNToFlush = null;

        Iterator<OWALPage> pageIterator = pagesCache.iterator();
        while (flushedPages < maxSize) {
          final OWALPage page = pageIterator.next();
          synchronized (page) {
            final int filledUpTo = page.getFilledUpTo();
            int pos = OWALPage.RECORDS_OFFSET;

            while (pos < filledUpTo) {
              if (!page.mergeWithNextPage(pos)) {
                if (pos == OWALPage.RECORDS_OFFSET && pendingLSNToFlush != null) {
                  lastLSNToFlush = pendingLSNToFlush;

                  pendingLSNToFlush = null;
                } else
                  lastLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * OWALPage.PAGE_SIZE + pos);
              } else if (pendingLSNToFlush == null)
                pendingLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * OWALPage.PAGE_SIZE + pos);

              pos += page.getSerializedRecordSize(pos);
            }

            ODirectMemoryPointer dataPointer;
            if (flushedPages == maxSize - 1) {
              dataPointer = new ODirectMemoryPointer(OWALPage.PAGE_SIZE);
              page.getPagePointer().moveData(0, dataPointer, 0, OWALPage.PAGE_SIZE);
            } else {
              dataPointer = page.getPagePointer();
            }

            pagesToFlush[flushedPages] = dataPointer;
          }

          flushedPages++;
        }

        synchronized (rndFile) {
          rndFile.seek(filePointer);
          for (int i = 0; i < pagesToFlush.length; i++) {
            ODirectMemoryPointer dataPointer = pagesToFlush[i];
            byte[] pageContent = dataPointer.get(0, OWALPage.PAGE_SIZE);
            if (i == pagesToFlush.length - 1)
              dataPointer.free();

            flushPage(pageContent);
            filePointer += OWALPage.PAGE_SIZE;
          }

          if (OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.getValueAsBoolean())
            rndFile.getFD().sync();
        }

        nextPositionToFlush = filePointer - OWALPage.PAGE_SIZE;

        if (lastLSNToFlush != null)
          flushedLsn = lastLSNToFlush;

        for (int i = 0; i < flushedPages - 1; i++) {
          OWALPage page = pagesCache.poll();
          page.getPagePointer().free();
        }

        assert !pagesCache.isEmpty();

        final long freeSpace = walLocation.getFreeSpace();
        if (freeSpace < freeSpaceLimit) {
          for (WeakReference<OLowDiskSpaceListener> listenerWeakReference : lowDiskSpaceListeners) {
            final OLowDiskSpaceListener lowDiskSpaceListener = listenerWeakReference.get();

            if (lowDiskSpaceListener != null)
              lowDiskSpaceListener.lowDiskSpace(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));
          }
        }
      }

      private void flushPage(byte[] content) throws IOException {
        CRC32 crc32 = new CRC32();
        crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);
        OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), content, 0);

        rndFile.write(content);
      }
    }

    private LogSegment(File file, int maxPagesCacheSize) throws IOException {
      this.file = file;
      this.maxPagesCacheSize = maxPagesCacheSize;

      order = extractOrder(file.getName());
      closed = false;
      rndFile = new RandomAccessFile(file, "rw");
    }

    public void startFlush() {
      if (commitDelay > 0)
        commitExecutor.scheduleAtFixedRate(new FlushTask(), commitDelay, commitDelay, TimeUnit.MILLISECONDS);
    }

    public void stopFlush(boolean flush) {
      if (flush)
        flush();

      if (!commitExecutor.isShutdown()) {
        commitExecutor.shutdown();
        try {
          if (!commitExecutor
              .awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
            throw new OStorageException("WAL flush task for '" + getPath() + "' segment cannot be stopped");

        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread");
        }
      }
    }

    public long getOrder() {
      return order;
    }

    public void init() throws IOException {
      selfCheck();

      initPageCache();

      last = new OLogSequenceNumber(order, filledUpTo - 1);
    }

    @Override
    public int compareTo(LogSegment other) {
      final long otherOrder = other.order;

      if (order > otherOrder)
        return 1;
      else if (order < otherOrder)
        return -1;

      return 0;
    }

    public long filledUpTo() throws IOException {
      return filledUpTo;
    }

    public OLogSequenceNumber begin() throws IOException {
      if (!pagesCache.isEmpty())
        return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

      if (rndFile.length() > 0)
        return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

      return null;
    }

    public OLogSequenceNumber end() {
      return last;
    }

    public void delete(boolean flush) throws IOException {
      close(flush);

      boolean deleted = OFileUtils.delete(file);
      int retryCount = 0;

      while (!deleted) {
        deleted = OFileUtils.delete(file);
        retryCount++;

        if (retryCount > 10)
          throw new IOException("Cannot delete file. Retry limit exceeded. (" + retryCount + ")");
      }
    }

    public String getPath() {
      return file.getAbsolutePath();
    }

    public OLogSequenceNumber logRecord(byte[] record) throws IOException {
      flushNewData = true;
      int pageOffset = (int) (filledUpTo % OWALPage.PAGE_SIZE);
      long pageIndex = filledUpTo / OWALPage.PAGE_SIZE;

      if (pageOffset == 0 && pageIndex > 0)
        pageIndex--;

      int pos = 0;
      boolean firstChunk = true;

      OLogSequenceNumber lsn = null;

      while (pos < record.length) {
        if (currentPage == null) {
          ODirectMemoryPointer pointer = new ODirectMemoryPointer(OWALPage.PAGE_SIZE);
          currentPage = new OWALPage(pointer, true);
          pagesCache.add(currentPage);
          filledUpTo += OWALPage.RECORDS_OFFSET;
        }

        int freeSpace = currentPage.getFreeSpace();
        if (freeSpace < OWALPage.MIN_RECORD_SIZE) {
          filledUpTo += freeSpace + OWALPage.RECORDS_OFFSET;
          ODirectMemoryPointer pointer = new ODirectMemoryPointer(OWALPage.PAGE_SIZE);
          currentPage = new OWALPage(pointer, true);
          pagesCache.add(currentPage);
          pageIndex++;

          freeSpace = currentPage.getFreeSpace();
        }

        final OWALPage walPage = currentPage;
        synchronized (walPage) {
          final int entrySize = OWALPage.calculateSerializedSize(record.length - pos);
          int addedChunkOffset;
          if (entrySize <= freeSpace) {
            if (pos == 0)
              addedChunkOffset = walPage.appendRecord(record, false, !firstChunk);
            else
              addedChunkOffset = walPage.appendRecord(Arrays.copyOfRange(record, pos, record.length), false, !firstChunk);

            pos = record.length;
          } else {
            int chunkSize = OWALPage.calculateRecordSize(freeSpace);
            if (chunkSize > record.length - pos)
              chunkSize = record.length - pos;

            addedChunkOffset = walPage.appendRecord(Arrays.copyOfRange(record, pos, pos + chunkSize), true, !firstChunk);
            pos += chunkSize;
          }

          if (firstChunk)
            lsn = new OLogSequenceNumber(order, pageIndex * OWALPage.PAGE_SIZE + addedChunkOffset);

          int spaceDiff = freeSpace - walPage.getFreeSpace();
          filledUpTo += spaceDiff;

          firstChunk = false;
        }
      }

      if (pagesCache.size() > maxPagesCacheSize) {
        OLogManager.instance().info(this, "Max cache limit is reached (%d vs. %d), sync flush is performed", maxPagesCacheSize,
            pagesCache.size());
        flush();
      }

      last = lsn;
      return last;
    }

    public byte[] readRecord(OLogSequenceNumber lsn) throws IOException {
      final OPair<OLogSequenceNumber, byte[]> lastRecord = lastReadRecord.get();
      if (lastRecord != null && lastRecord.getKey().equals(lsn))
        return lastRecord.getValue();

      assert lsn.getSegment() == order;
      if (lsn.getPosition() >= filledUpTo)
        return null;

      if (!pagesCache.isEmpty())
        flush();

      long pageIndex = lsn.getPosition() / OWALPage.PAGE_SIZE;

      byte[] record = null;
      int pageOffset = (int) (lsn.getPosition() % OWALPage.PAGE_SIZE);

      long pageCount = (filledUpTo + OWALPage.PAGE_SIZE - 1) / OWALPage.PAGE_SIZE;

      while (pageIndex < pageCount) {
        byte[] pageContent = new byte[OWALPage.PAGE_SIZE];
        synchronized (rndFile) {
          rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
          rndFile.readFully(pageContent);
        }

        if (!checkPageIntegrity(pageContent))
          throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

        ODirectMemoryPointer pointer = new ODirectMemoryPointer(pageContent);
        try {
          OWALPage page = new OWALPage(pointer, false);

          byte[] content = page.getRecord(pageOffset);
          if (record == null)
            record = content;
          else {
            byte[] oldRecord = record;

            record = new byte[record.length + content.length];
            System.arraycopy(oldRecord, 0, record, 0, oldRecord.length);
            System.arraycopy(content, 0, record, oldRecord.length, record.length - oldRecord.length);
          }

          if (page.mergeWithNextPage(pageOffset)) {
            pageOffset = OWALPage.RECORDS_OFFSET;
            pageIndex++;
            if (pageIndex >= pageCount)
              throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");
          } else {
            if (page.getFreeSpace() >= OWALPage.MIN_RECORD_SIZE && pageIndex < pageCount - 1)
              throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

            break;
          }
        } finally {
          pointer.free();
        }
      }

      lastReadRecord = new WeakReference<OPair<OLogSequenceNumber, byte[]>>(new OPair<OLogSequenceNumber, byte[]>(lsn, record));
      return record;
    }

    public OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException {
      final byte[] record = readRecord(lsn);
      if (record == null)
        return null;

      long pos = lsn.getPosition();
      long pageIndex = pos / OWALPage.PAGE_SIZE;
      int pageOffset = (int) (pos - pageIndex * OWALPage.PAGE_SIZE);

      int restOfRecord = record.length;
      while (restOfRecord > 0) {
        int entrySize = OWALPage.calculateSerializedSize(restOfRecord);
        if (entrySize + pageOffset < OWALPage.PAGE_SIZE) {
          if (entrySize + pageOffset <= OWALPage.PAGE_SIZE - OWALPage.MIN_RECORD_SIZE)
            pos += entrySize;
          else
            pos += OWALPage.PAGE_SIZE - pageOffset + OWALPage.RECORDS_OFFSET;
          break;
        } else if (entrySize + pageOffset == OWALPage.PAGE_SIZE) {
          pos += entrySize + OWALPage.RECORDS_OFFSET;
          break;
        } else {
          int chunkSize = OWALPage.calculateRecordSize(OWALPage.PAGE_SIZE - pageOffset);
          restOfRecord -= chunkSize;

          pos += OWALPage.PAGE_SIZE - pageOffset + OWALPage.RECORDS_OFFSET;
          pageOffset = OWALPage.RECORDS_OFFSET;
        }
      }

      if (pos >= filledUpTo)
        return null;

      return new OLogSequenceNumber(order, pos);
    }

    public void close(boolean flush) throws IOException {
      if (!closed) {
        lastReadRecord.clear();

        stopFlush(flush);

        rndFile.close();

        closed = true;

        if (!pagesCache.isEmpty()) {
          for (OWALPage page : pagesCache)
            page.getPagePointer().free();
        }

        currentPage = null;
      }
    }

    public OLogSequenceNumber readFlushedLSN() throws IOException {
      long pages = rndFile.length() / OWALPage.PAGE_SIZE;
      if (pages == 0)
        return null;

      return new OLogSequenceNumber(order, filledUpTo - 1);
    }

    public void flush() {
      if (!commitExecutor.isShutdown()) {
        try {
          commitExecutor.submit(new FlushTask()).get();
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new OStorageException("Thread was interrupted during flush", e);
        } catch (ExecutionException e) {
          throw new OStorageException("Error during WAL segment '" + getPath() + "' flush", e);
        }
      } else {
        new FlushTask().run();
      }
    }

    private void initPageCache() throws IOException {
      synchronized (rndFile) {
        long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;
        if (pagesCount == 0)
          return;

        rndFile.seek((pagesCount - 1) * OWALPage.PAGE_SIZE);
        byte[] content = new byte[OWALPage.PAGE_SIZE];
        rndFile.readFully(content);

        if (checkPageIntegrity(content)) {
          ODirectMemoryPointer pointer = new ODirectMemoryPointer(content);
          currentPage = new OWALPage(pointer, false);
          filledUpTo = (pagesCount - 1) * OWALPage.PAGE_SIZE + currentPage.getFilledUpTo();
          nextPositionToFlush = (pagesCount - 1) * OWALPage.PAGE_SIZE;
        } else {
          ODirectMemoryPointer pointer = new ODirectMemoryPointer(OWALPage.PAGE_SIZE);
          currentPage = new OWALPage(pointer, true);
          filledUpTo = pagesCount * OWALPage.PAGE_SIZE + currentPage.getFilledUpTo();
          nextPositionToFlush = pagesCount * OWALPage.PAGE_SIZE;
        }

        pagesCache.add(currentPage);
      }
    }

    private long extractOrder(String name) {
      final Matcher matcher = Pattern.compile("^.*\\.(\\d+)\\.wal$").matcher(name);

      final boolean matches = matcher.find();
      assert matches;

      final String order = matcher.group(1);
      try {
        return Long.parseLong(order);
      } catch (NumberFormatException e) {
        // never happen
        throw new IllegalStateException(e);
      }
    }

    private boolean checkPageIntegrity(byte[] content) {
      final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, OWALPage.MAGIC_NUMBER_OFFSET);
      if (magicNumber != OWALPage.MAGIC_NUMBER)
        return false;

      final CRC32 crc32 = new CRC32();
      crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);

      return ((int) crc32.getValue()) == OIntegerSerializer.INSTANCE.deserializeNative(content, 0);
    }

    private void selfCheck() throws IOException {
      if (!pagesCache.isEmpty())
        throw new IllegalStateException("WAL cache is not empty, we cannot verify WAL after it was started to be used");

      synchronized (rndFile) {
        long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;

        if (rndFile.length() % OWALPage.PAGE_SIZE > 0) {
          OLogManager.instance().error(this, "Last WAL page was written partially, auto fix");

          rndFile.setLength(OWALPage.PAGE_SIZE * pagesCount);
        }
      }
    }
  }

  public ODiskWriteAheadLog(OLocalPaginatedStorage storage) throws IOException {
    this(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(), OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger() * ONE_KB * ONE_KB, storage);
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

  public ODiskWriteAheadLog(int maxPagesCacheSize, int commitDelay, long maxSegmentSize, OLocalPaginatedStorage storage)
      throws IOException {
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitDelay = commitDelay;
    this.maxSegmentSize = maxSegmentSize;
    this.storage = storage;

    try {
      this.walLocation = new File(calculateWalPath(this.storage));

      File[] walFiles = this.walLocation.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return validateName(name);
        }
      });

      if (walFiles == null)
        throw new IllegalStateException(
            "Location passed in WAL does not exist, or IO error was happened. DB cannot work in durable mode in such case");

      if (walFiles.length == 0) {
        LogSegment logSegment = new LogSegment(new File(this.walLocation, getSegmentName(0)), maxPagesCacheSize);
        logSegment.init();
        logSegment.startFlush();
        logSegments.add(logSegment);

        logSize = 0;

        flushedLsn = null;
      } else {

        logSize = 0;

        for (File walFile : walFiles) {
          LogSegment logSegment = new LogSegment(walFile, maxPagesCacheSize);
          logSegment.init();

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

  private static String calculateWalPath(OLocalPaginatedStorage storage) {
    String walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
    if (walPath == null)
      walPath = storage.getStoragePath();

    return walPath;
  }

  public static boolean validateName(String name) {
    if (!name.toLowerCase().endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');

    if (walOrderStartIndex == name.length() - 4)
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

  public File getWalLocation() {
    return walLocation;
  }

  public OLogSequenceNumber begin() throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      LogSegment first = logSegments.get(0);
      if (first.filledUpTo() == 0)
        return null;

      return first.begin();

    } finally {
      syncObject.unlock();
    }
  }

  public OLogSequenceNumber end() throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      int lastIndex = logSegments.size() - 1;
      LogSegment last = logSegments.get(lastIndex);
      while (last.filledUpTo == 0) {
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
    LogSegment last;

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
    syncObject.lock();
    try {
      checkForClose();

      final OLogSequenceNumber lsn = log(new OAtomicUnitStartRecord(isRollbackSupported, unitId));
      activeOperations.add(unitId);
      return lsn;
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback,
      OLogSequenceNumber startLsn) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      final OLogSequenceNumber lsn = log(new OAtomicUnitEndRecord(operationUnitId, rollback, startLsn));
      activeOperations.remove(operationUnitId);

      return lsn;
    } finally {
      syncObject.unlock();
    }
  }

  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      if (segmentCreationFlag && record instanceof OOperationUnitRecord
          && !activeOperations.contains(((OOperationUnitRecord) record).getOperationUnitId())) {
        while (segmentCreationFlag) {
          try {
            segmentCreationComplete.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OInterruptedException(e);
          }
        }
      }

      final byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(record);

      LogSegment last = logSegments.get(logSegments.size() - 1);
      long lastSize = last.filledUpTo();

      final OLogSequenceNumber lsn = last.logRecord(serializedForm);
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

        if (record instanceof OAtomicUnitEndRecord && activeOperations.size() == 1
            || (!(record instanceof OOperationUnitRecord) && activeOperations.isEmpty())) {
          last.stopFlush(true);

          last = new LogSegment(new File(walLocation, getSegmentName(last.getOrder() + 1)), maxPagesCacheSize);
          last.init();
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

  public long size() {
    syncObject.lock();
    try {
      return logSize;
    } finally {
      syncObject.unlock();
    }
  }

  public List<String> getWalFiles() {
    final ArrayList<String> result = new ArrayList<String>();
    syncObject.lock();
    try {
      for (LogSegment segment : logSegments) {
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

      ListIterator<LogSegment> iterator = logSegments.listIterator(logSegments.size() - 1);
      while (iterator.hasPrevious()) {
        final LogSegment logSegment = iterator.previous();
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

      for (LogSegment logSegment : logSegments)
        logSegment.close(flush);

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

      for (LogSegment logSegment : logSegments)
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

      LogSegment logSegment = logSegments.get(index);
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

      LogSegment logSegment = logSegments.get(index);
      OLogSequenceNumber nextLSN = logSegment.getNextLSN(lsn);

      if (nextLSN == null) {
        index++;
        if (index >= logSegments.size())
          return null;

        LogSegment nextSegment = logSegments.get(index);
        if (nextSegment.filledUpTo() == 0)
          return null;

        nextLSN = nextSegment.begin();
      }

      return nextLSN;
    } finally {
      syncObject.unlock();
    }
  }

  public OLogSequenceNumber getFlushedLSN() {
    return flushedLsn;
  }

  public void cutTill(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      flush();

      int lastTruncateIndex = -1;

      for (int i = 0; i < logSegments.size() - 1; i++) {
        final LogSegment logSegment = logSegments.get(i);

        if (logSegment.end().compareTo(lsn) < 0)
          lastTruncateIndex = i;
        else
          break;
      }

      for (int i = 0; i <= lastTruncateIndex; i++) {
        final LogSegment logSegment = removeHeadSegmentFromList();
        if (logSegment != null)
          logSegment.delete(false);
      }

      recalculateLogSize();
      fixMasterRecords();
    } finally {
      syncObject.unlock();
    }
  }

  private LogSegment removeHeadSegmentFromList() {
    if (logSegments.size() < 2)
      return null;

    return logSegments.remove(0);
  }

  private void recalculateLogSize() throws IOException {
    logSize = 0;

    for (LogSegment segment : logSegments)
      logSize += segment.filledUpTo();
  }

  private void fixMasterRecords() throws IOException {
    if (firstMasterRecord != null) {
      int index = (int) (firstMasterRecord.getSegment() - logSegments.get(0).getOrder());
      if (logSegments.size() <= index || index < 0) {
        firstMasterRecord = null;
      } else {
        LogSegment firstMasterRecordSegment = logSegments.get(index);
        if (firstMasterRecordSegment.filledUpTo() <= firstMasterRecord.getPosition())
          firstMasterRecord = null;
      }
    }

    if (secondMasterRecord != null) {
      int index = (int) (secondMasterRecord.getSegment() - logSegments.get(0).getOrder());
      if (logSegments.size() <= index || index < 0) {
        secondMasterRecord = null;
      } else {
        LogSegment secondMasterRecordSegment = logSegments.get(index);
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
      masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE));

      int firstCRC = masterRecordLSNHolder.readInt();
      final long segment = masterRecordLSNHolder.readLong();
      final long position = masterRecordLSNHolder.readLong();

      byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serializeLiteral(segment, serializedLSN, 0);
      OLongSerializer.INSTANCE.serializeLiteral(position, serializedLSN, OLongSerializer.LONG_SIZE);
      crc32.update(serializedLSN);

      if (firstCRC != ((int) crc32.getValue())) {
        OLogManager.instance().error(this, "Cannot restore %d WAL master record for storage %s crc check is failed", index,
            storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (EOFException eofException) {
      OLogManager.instance().debug(this, "Cannot restore %d WAL master record for storage %s", index, storageName);
      return null;
    }
  }

  private void writeMasterRecord(int index, OLogSequenceNumber masterRecord) throws IOException {
    masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE));
    final CRC32 crc32 = new CRC32();

    final byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getPosition(), serializedLSN, OLongSerializer.LONG_SIZE);
    crc32.update(serializedLSN);

    masterRecordLSNHolder.writeInt((int) crc32.getValue());
    masterRecordLSNHolder.writeLong(masterRecord.getSegment());
    masterRecordLSNHolder.writeLong(masterRecord.getPosition());
  }

  private String getSegmentName(long order) {
    return storage.getName() + "." + order + WAL_SEGMENT_EXTENSION;
  }

  private OLogSequenceNumber readFlushedLSN() throws IOException {
    int segment = logSegments.size() - 1;
    while (segment >= 0) {
      LogSegment logSegment = logSegments.get(segment);
      OLogSequenceNumber flushedLSN = logSegment.readFlushedLSN();
      if (flushedLSN == null)
        segment--;
      else
        return flushedLSN;
    }

    return null;
  }

}
