/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.*;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class OWriteAheadLog {
  private static final long           ONE_KB                  = 1024L;

  public static final String          MASTER_RECORD_EXTENSION = ".wmr";
  public static final String          WAL_SEGMENT_EXTENSION   = ".wal";

  private OLogSequenceNumber          lastCheckpoint;

  private final Object                syncObject              = new Object();

  private final List<LogSegment>      logSegments             = new ArrayList<LogSegment>();

  private boolean                     useFirstMasterRecord    = true;

  private final int                   maxPagesCacheSize;
  private final int                   commitDelay;

  private final long                  maxSegmentSize;
  private final long                  maxLogSize;

  private long                        logSize;

  private final File                  walLocation;

  private File                        masterRecordFile;
  private final RandomAccessFile      masterRecordLSNHolder;

  private OLogSequenceNumber          firstMasterRecord;
  private OLogSequenceNumber          secondMasterRecord;

  private volatile OLogSequenceNumber flushedLsn;

  private final OStorageLocalAbstract storage;

  private boolean                     closed;

  private static String calculateWalPath(OStorageLocalAbstract storage) {
    String walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
    if (walPath == null)
      walPath = storage.getStoragePath();

    return walPath;
  }

  public OWriteAheadLog(OStorageLocalAbstract storage) throws IOException {
    this(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger(), OGlobalConfiguration.WAL_COMMIT_TIMEOUT.getValueAsInteger(),
        OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsInteger() * ONE_KB * ONE_KB, OGlobalConfiguration.WAL_MAX_SIZE
            .getValueAsInteger() * ONE_KB * ONE_KB, storage);
  }

  public OWriteAheadLog(int maxPagesCacheSize, int commitDelay, long maxSegmentSize, long maxLogSize, OStorageLocalAbstract storage)
      throws IOException {
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitDelay = commitDelay;
    this.maxSegmentSize = maxSegmentSize;
    this.maxLogSize = maxLogSize;
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
            "Location passed in WAL does not exist, or IO error was happened. DB can not work in durable mode in such case.");

      if (walFiles.length == 0) {
        LogSegment logSegment = new LogSegment(new File(this.walLocation, getSegmentName(0)), maxPagesCacheSize);
        logSegment.init();
        logSegment.startFlush();
        logSegments.add(logSegment);

        logSize = 0;

        flushedLsn = null;
      } else {

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
      OLogManager.instance().error(this, "Error during file initialization for storage %s", e, this.storage.getName());
      throw new IllegalStateException("Error during file initialization for storage " + this.storage.getName(), e);
    }
  }

  public File getWalLocation() {
    return walLocation;
  }

  public OLogSequenceNumber begin() throws IOException {
    synchronized (syncObject) {
      checkForClose();

      LogSegment first = logSegments.get(0);
      if (first.filledUpTo() == 0)
        return null;

      return first.begin();
    }
  }

  public OLogSequenceNumber end() throws IOException {
    synchronized (syncObject) {
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
    }
  }

  public void flush() {
    synchronized (syncObject) {
      checkForClose();

      LogSegment last = logSegments.get(logSegments.size() - 1);
      last.flush();
    }
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
    CRC32 crc32 = new CRC32();
    try {
      masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE));

      int firstCRC = masterRecordLSNHolder.readInt();
      long segment = masterRecordLSNHolder.readLong();
      long position = masterRecordLSNHolder.readLong();

      byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serialize(segment, serializedLSN, 0);
      OLongSerializer.INSTANCE.serialize(position, serializedLSN, OLongSerializer.LONG_SIZE);
      crc32.update(serializedLSN);

      if (firstCRC != ((int) crc32.getValue())) {
        OLogManager.instance().error(this, "Can not restore %d WAL master record for storage %s crc check is failed", index,
            storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (EOFException eofException) {
      OLogManager.instance().warn(this, "Can not restore %d WAL master record for storage %s", index, storageName);
      return null;
    }
  }

  private void writeMasterRecord(int index, OLogSequenceNumber masterRecord) throws IOException {
    masterRecordLSNHolder.seek(index * (OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE));
    CRC32 crc32 = new CRC32();

    byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serialize(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serialize(masterRecord.getPosition(), serializedLSN, OLongSerializer.LONG_SIZE);
    crc32.update(serializedLSN);

    masterRecordLSNHolder.writeInt((int) crc32.getValue());
    masterRecordLSNHolder.writeLong(masterRecord.getSegment());
    masterRecordLSNHolder.writeLong(masterRecord.getPosition());
  }

  private String getSegmentName(long order) {
    return storage.getName() + "." + order + WAL_SEGMENT_EXTENSION;
  }

  public OLogSequenceNumber logFuzzyCheckPointStart() throws IOException {
    synchronized (syncObject) {
      checkForClose();

      OFuzzyCheckpointStartRecord record = new OFuzzyCheckpointStartRecord(lastCheckpoint);
      log(record);
      return record.getLsn();
    }
  }

  public OLogSequenceNumber logFuzzyCheckPointEnd() throws IOException {
    synchronized (syncObject) {
      checkForClose();

      OFuzzyCheckpointEndRecord record = new OFuzzyCheckpointEndRecord();
      log(record);
      return record.getLsn();
    }
  }

  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    synchronized (syncObject) {
      checkForClose();

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

      if (logSize >= maxLogSize) {
        LogSegment first = logSegments.get(0);
        first.stopFlush(false);

        logSize -= first.filledUpTo();

        first.delete(false);
        logSegments.remove(0);

        fixMasterRecords();
      }

      if (last.filledUpTo() >= maxSegmentSize) {
        last.stopFlush(true);

        last = new LogSegment(new File(walLocation, getSegmentName(last.getOrder() + 1)), maxPagesCacheSize);
        last.init();
        last.startFlush();

        logSegments.add(last);
      }

      return lsn;
    }
  }

  public long size() {
    synchronized (syncObject) {
      return logSize;
    }
  }

  public void truncate() throws IOException {
    synchronized (syncObject) {
      if (logSegments.size() < 2)
        return;

      ListIterator<LogSegment> iterator = logSegments.listIterator(logSegments.size() - 1);
      while (iterator.hasPrevious()) {
        final LogSegment logSegment = iterator.previous();
        logSegment.delete(false);
        iterator.remove();
      }
    }
  }

  public void close() throws IOException {
    close(true);
  }

  public void close(boolean flush) throws IOException {
    synchronized (syncObject) {
      if (closed)
        return;

      closed = true;

      for (LogSegment logSegment : logSegments)
        logSegment.close(flush);

      masterRecordLSNHolder.close();
    }

  }

  private void checkForClose() {
    if (closed)
      throw new OStorageException("WAL log " + walLocation + " has been closed");
  }

  public void delete() throws IOException {
    delete(false);
  }

  public void delete(boolean flush) throws IOException {
    synchronized (syncObject) {
      close(flush);

      for (LogSegment logSegment : logSegments)
        logSegment.delete(false);

      boolean deleted = masterRecordFile.delete();
      while (!deleted) {
        OMemoryWatchDog.freeMemoryForResourceCleanup(100);
        deleted = !masterRecordFile.exists() || masterRecordFile.delete();
      }
    }
  }

  public void logDirtyPages(Set<ODirtyPage> dirtyPages) throws IOException {
    synchronized (syncObject) {
      checkForClose();

      log(new ODirtyPagesRecord(dirtyPages));
    }
  }

  public OLogSequenceNumber getLastCheckpoint() {
    synchronized (syncObject) {
      checkForClose();

      return lastCheckpoint;
    }
  }

  public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
    synchronized (syncObject) {
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
    }
  }

  public OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException {
    synchronized (syncObject) {
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
    }
  }

  public OLogSequenceNumber getFlushedLSN() {
    synchronized (syncObject) {
      checkForClose();

      return flushedLsn;
    }
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

  public OLogSequenceNumber logFullCheckpointStart() throws IOException {
    return log(new OFullCheckpointStartRecord(lastCheckpoint));
  }

  public void logFullCheckpointEnd() throws IOException {
    synchronized (syncObject) {
      checkForClose();

      log(new OCheckpointEndRecord());
    }
  }

  private final class LogSegment implements Comparable<LogSegment> {
    private final RandomAccessFile                rndFile;
    private final File                            file;

    private long                                  filledUpTo;

    private final long                            order;
    private final int                             maxPagesCacheSize;
    private boolean                               closed;

    private OWALPage                              currentPage;
    private final ConcurrentLinkedQueue<OWALPage> pagesCache     = new ConcurrentLinkedQueue<OWALPage>();

    private long                                  nextPositionToFlush;

    private final ScheduledExecutorService        commitExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                                                                   @Override
                                                                   public Thread newThread(Runnable r) {
                                                                     Thread thread = new Thread(r);
                                                                     thread.setDaemon(true);
                                                                     thread.setName("WAL Flush Task");
                                                                     return thread;
                                                                   }
                                                                 });

    private OLogSequenceNumber                    last           = null;
    private OLogSequenceNumber                    pendingLSNToFlush;

    private volatile boolean                      flushNewData   = true;
    private WeakReference<HashMap<Long, byte[]>>  readCache      = new WeakReference<HashMap<Long, byte[]>>(
                                                                     new HashMap<Long, byte[]>());

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
            throw new OStorageException("WAL flush task for " + getPath() + " segment can not be stopped.");

        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Can not shutdown background WAL commit thread.");
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
      int walOrderStartIndex = name.indexOf('.') + 1;

      int walOrderEndIndex = name.indexOf('.', walOrderStartIndex);
      String walOrder = name.substring(walOrderStartIndex, walOrderEndIndex);
      try {
        return Long.parseLong(walOrder);
      } catch (NumberFormatException e) {
        // never happen
        throw new IllegalStateException(e);
      }
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

      boolean deleted = file.delete();
      while (!deleted) {
        OMemoryWatchDog.freeMemoryForResourceCleanup(100);
        deleted = !file.exists() || file.delete();
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
        OLogManager.instance().info(this, "Max cache limit is reached (%d vs. %d), sync flush is performed.", maxPagesCacheSize,
            pagesCache.size());
        flush();
      }

      last = lsn;
      return last;
    }

    public byte[] readRecord(OLogSequenceNumber lsn) throws IOException {
      assert lsn.getSegment() == order;

      if (lsn.getPosition() >= filledUpTo)
        return null;

      long pageIndex = lsn.getPosition() / OWALPage.PAGE_SIZE;

      if (flushedLsn == null || flushedLsn.getSegment() < order
          || (flushedLsn.getSegment() == order && flushedLsn.getPosition() / OWALPage.PAGE_SIZE <= pageIndex)) {
        flush();
      }

      byte[] record = null;
      int pageOffset = (int) (lsn.getPosition() % OWALPage.PAGE_SIZE);

      long pageCount = (filledUpTo + OWALPage.PAGE_SIZE - 1) / OWALPage.PAGE_SIZE;

      while (pageIndex < pageCount) {
        byte[] pageContent = getPageFromCache(pageIndex);

        if (!checkPageIntegrity(pageContent))
          throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken.");

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
              throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken.");
          } else {
            if (page.getFreeSpace() >= OWALPage.MIN_RECORD_SIZE && pageIndex < pageCount - 1)
              throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken.");

            break;
          }
        } finally {
          pointer.free();
        }
      }

      return record;
    }

    private boolean checkPageIntegrity(byte[] content) {
      final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, OWALPage.MAGIC_NUMBER_OFFSET);
      if (magicNumber != OWALPage.MAGIC_NUMBER)
        return false;

      final CRC32 crc32 = new CRC32();
      crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);

      return ((int) crc32.getValue()) == OIntegerSerializer.INSTANCE.deserializeNative(content, 0);
    }

    private HashMap<Long, byte[]> readDataInCache(long pageIndex) throws IOException {
      final long pageCount = rndFile.length() / OWALPage.PAGE_SIZE;
      final long cacheSize = OGlobalConfiguration.WAL_READ_CACHE_SIZE.getValueAsInteger();

      final long endPage = Math.min(pageCount, pageIndex + cacheSize);

      final HashMap<Long, byte[]> result = new HashMap<Long, byte[]>((int) (endPage - pageIndex));

      rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
      while (pageIndex < endPage) {
        synchronized (rndFile) {
          byte[] pageContent = new byte[OWALPage.PAGE_SIZE];
          rndFile.readFully(pageContent);
          result.put(pageIndex, pageContent);
          pageIndex++;
        }
      }

      return result;
    }

    private byte[] getPageFromCache(long pageIndex) throws IOException {
      HashMap<Long, byte[]> pageCache = readCache.get();
      if (pageCache != null && pageCache.containsKey(pageIndex))
        return pageCache.get(pageIndex);

      pageCache = readDataInCache(pageIndex);
      readCache = new WeakReference<HashMap<Long, byte[]>>(pageCache);
      return pageCache.get(pageIndex);
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

    private void selfCheck() throws IOException {
      if (!pagesCache.isEmpty())
        throw new IllegalStateException("WAL cache is not empty, we can not verify WAL after it was started to be used");

      synchronized (rndFile) {
        long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;

        if (rndFile.length() % OWALPage.PAGE_SIZE > 0) {
          OLogManager.instance().error(this, "Last WAL page was written partially, auto fix.");

          rndFile.setLength(OWALPage.PAGE_SIZE * pagesCount);
        }
      }
    }

    public OLogSequenceNumber readFlushedLSN() throws IOException {
      long pages = rndFile.length() / OWALPage.PAGE_SIZE;
      if (pages == 0)
        return null;

      return new OLogSequenceNumber(order, filledUpTo - 1);
    }

    public void flush() {
      final HashMap<Long, byte[]> pageCache = readCache.get();
      if (pageCache != null)
        pageCache.clear();

      if (!commitExecutor.isShutdown()) {
        try {
          commitExecutor.submit(new FlushTask()).get();
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new OStorageException("Thread was interrupted during flush", e);
        } catch (ExecutionException e) {
          throw new OStorageException("Error during WAL segment " + getPath() + " flush.");
        }
      } else {
        new FlushTask().run();
      }
    }

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
      }

      private void flushPage(byte[] content) throws IOException {
        CRC32 crc32 = new CRC32();
        crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);
        OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), content, 0);

        rndFile.write(content);
      }
    }
  }

}
