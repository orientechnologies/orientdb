package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

final class OLogSegmentV2 implements OLogSegment {
  private final ODiskWriteAheadLog writeAheadLog;

  private volatile long writtenUpTo;

  private volatile OLogSequenceNumber storedUpTo;
  private volatile OLogSequenceNumber syncedUpTo;

  private final Path path;
  private final long order;
  private final int  maxPagesCacheSize;

  private final OWALSegmentCache segmentCache;

  private final    Lock             cacheLock  = new ReentrantLock();
  private volatile List<OLogRecord> writeCache = new ArrayList<>();

  private final ScheduledExecutorService commitExecutor;

  private volatile long    filledUpTo;
  private          boolean closed;
  private OLogSequenceNumber last = null;

  private volatile boolean flushNewData = true;

  private WeakReference<OPair<OLogSequenceNumber, byte[]>> lastReadRecord = new WeakReference<>(null);

  private final class WriteTask implements Runnable {
    private WriteTask() {
    }

    @Override
    public void run() {
      try {
        if (!flushNewData)
          return;

        flushNewData = false;

        List<OLogRecord> toFlush;
        try {
          cacheLock.lock();
          if (writeCache.isEmpty())
            return;

          toFlush = writeCache;
          writeCache = new ArrayList<>();
        } finally {
          cacheLock.unlock();
        }

        ByteBuffer buffer;
        OLogRecord first = toFlush.get(0);
        long curPageIndex = first.writeFrom / OWALPage.PAGE_SIZE;

        //Position of the last record end of which is stored inside of the page
        //We use it to find the last log record which is flushed for sure at the end of
        //the WAL when we open WAL after storage was closed or crashed.
        long lastRecordPosition = -1;

        //Position of the end of last record inside of the page
        int endOfLastRecord = -1;

        final long filledUpTo = segmentCache.filledUpTo();
        if (filledUpTo > curPageIndex) {
          assert filledUpTo - 1 == curPageIndex;

          buffer = segmentCache.readPageBuffer(curPageIndex);

          //reread the value of last updated page otherwise it will be overwritten
          //by -1 later
          lastRecordPosition = buffer.getLong(OWALPageV2.LAST_STORED_LSN);
          endOfLastRecord = buffer.getInt(OWALPageV2.END_LAST_RECORD);
        } else {
          buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

          buffer.putLong(OWALPageV2.LAST_STORED_LSN, lastRecordPosition);
          buffer.putInt(OWALPageV2.END_LAST_RECORD, endOfLastRecord);
        }

        OLogSequenceNumber lsn;
        boolean lastToFlush = false;

        for (OLogRecord log : toFlush) {
          lsn = new OLogSequenceNumber(order, log.writeFrom);

          int pos = (int) (log.writeFrom % OWALPage.PAGE_SIZE);
          int written = 0;

          while (written < log.record.length) {
            lastToFlush = true;

            int pageFreeSpace = OWALPageV2.calculateRecordSize(OWALPage.PAGE_SIZE - pos);
            int contentLength = Math.min(pageFreeSpace, (log.record.length - written));
            int fromRecord = written;
            written += contentLength;

            //end of the record is reached in this page
            //lets remember that and write to the page
            if (written == log.record.length) {
              lastRecordPosition = lsn.getPosition();
              endOfLastRecord = pos + OWALPageV2.calculateSerializedSize(contentLength);
            }

            pos = writeContentInPage(buffer, pos, log.record, written == log.record.length, fromRecord, contentLength,
                lastRecordPosition, endOfLastRecord);

            if (OWALPage.PAGE_SIZE - pos < OWALPage.MIN_RECORD_SIZE) {
              preparePageForFlush(buffer);

              segmentCache.writePage(buffer, curPageIndex);

              buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
              curPageIndex++;

              //new page for sure nothing is written there yet
              lastRecordPosition = -1;
              endOfLastRecord = -1;

              lastToFlush = false;

              pos = OWALPageV2.RECORDS_OFFSET;
            }
          }

          writtenUpTo = log.writeTo;
          storedUpTo = lsn;
        }

        if (lastToFlush) {
          preparePageForFlush(buffer);

          segmentCache.writePage(buffer, curPageIndex);
        }

        writeAheadLog.checkFreeSpace();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during WAL background flush", e);
      }
    }

    /**
     * Write the content in the page and return the new page cursor position.
     *
     * @param pageContent     buffer of the page to be filled
     * @param posInPage       position in the page where to write
     * @param log             content to write to the page
     * @param isLast          flag to mark if is last portion of the record
     * @param fromRecord      the start of the portion of the record to write in this page
     * @param contentLength   the length of the portion of the record to write in this page
     * @param lsnPosition     Position part of LSN of last stored record
     * @param endOfLastRecord End position of last record end of which is written in this page
     *
     * @return the new page cursor  position after this write.
     */
    private int writeContentInPage(ByteBuffer pageContent, int posInPage, byte[] log, boolean isLast, int fromRecord,
        int contentLength, long lsnPosition, int endOfLastRecord) {
      pageContent.put(posInPage, !isLast ? (byte) 1 : 0);
      pageContent.put(posInPage + 1, isLast ? (byte) 1 : 0);
      pageContent.putInt(posInPage + 2, contentLength);
      pageContent.position(posInPage + OIntegerSerializer.INT_SIZE + 2);
      pageContent.put(log, fromRecord, contentLength);

      posInPage += OWALPageV2.calculateSerializedSize(contentLength);

      pageContent.putInt(OWALPage.FREE_SPACE_OFFSET, OWALPage.PAGE_SIZE - posInPage);

      pageContent.putLong(OWALPageV2.LAST_STORED_LSN, lsnPosition);
      pageContent.putInt(OWALPageV2.END_LAST_RECORD, endOfLastRecord);

      return posInPage;
    }
  }

  private final class SyncTask implements Runnable {
    @Override
    public void run() {
      try {
        final OLogSequenceNumber stored = storedUpTo;
        final OLogSequenceNumber synced = syncedUpTo;

        if (stored == null) // nothing stored yet, so there is nothing to sync, exit
          return;

        if (synced == null || synced.compareTo(stored) < 0) { // it's a first sync request or we have a new data to sync
          segmentCache.sync();
          syncedUpTo = stored;
          writeAheadLog.setFlushedLsn(stored);
        }
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Can not force sync content of file " + path, ioe);
      }
    }
  }

  OLogSegmentV2(ODiskWriteAheadLog writeAheadLog, Path path, int maxPagesCacheSize, int fileTTL, int segmentBufferSize,
      ScheduledExecutorService closer, ScheduledExecutorService commitExecutor) throws IOException {
    this.writeAheadLog = writeAheadLog;
    this.path = path;
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitExecutor = commitExecutor;

    order = extractOrder(path.getFileName().toString());

    this.segmentCache = new OWALSegmentCache(path, fileTTL, segmentBufferSize, closer);
    this.segmentCache.open();

    closed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startBackgroundWrite() {
    if (writeAheadLog.getCommitDelay() > 0) {
      commitExecutor.scheduleAtFixedRate(new WriteTask(), 100, 100, TimeUnit.MICROSECONDS);
      commitExecutor.scheduleAtFixedRate(new SyncTask(), writeAheadLog.getCommitDelay(), writeAheadLog.getCommitDelay(),
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stopBackgroundWrite(boolean flush) {
    if (flush)
      flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
          throw new OStorageException("WAL flush task for '" + getPath() + "' segment cannot be stopped");

      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread", e);
      }
    }
  }

  private void preparePageForFlush(ByteBuffer content) {
    content.putLong(OWALPage.MAGIC_NUMBER_OFFSET, OWALPageV2.MAGIC_NUMBER);

    final byte[] data = new byte[OWALPage.PAGE_SIZE - OWALPage.MAGIC_NUMBER_OFFSET];
    content.position(OWALPage.MAGIC_NUMBER_OFFSET);
    content.get(data);

    CRC32 crc32 = new CRC32();
    crc32.update(data);
    content.putInt(OWALPage.CRC_OFFSET, (int) crc32.getValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getOrder() {
    return order;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() throws IOException {
    long currentPage;

    //position of LSN of last record which is fully written to the WAL
    long lastPosition;

    final long pages = segmentCache.filledUpTo();

    if (pages == 0) {
      last = null;
      filledUpTo = 0;

      return;
    }

    currentPage = pages;

    ByteBuffer buffer;

    //find first not broken page starting from the end which contains
    //the last fully written WAL record, sure there may be some broken pages in the middle
    //but in such way we will truncate all broken pages at the end
    while (true) {
      currentPage--;

      if (currentPage < 0) {
        last = null;
        filledUpTo = 0;

        OLogManager.instance()
            .error(this, "%d pages in WAL segment %s are broken and will be truncated, some data will be lost after restore.", null,
                pages, path.getFileName());

        segmentCache.truncate(0);
        segmentCache.sync();

        return;
      }

      final byte[] content = segmentCache.readPage(currentPage);
      buffer = ByteBuffer.wrap(content).order(ByteOrder.nativeOrder());

      if (isPageBroken(content)) {
        OLogManager.instance()
            .warn(this, "Page %d is broken in WAL segment %d and will be truncated", currentPage, currentPage, order);
      } else {
        lastPosition = buffer.getLong(OWALPageV2.LAST_STORED_LSN);
        if (lastPosition >= 0) {
          break;
        }
      }
    }

    if (currentPage + 1 < pages) {
      OLogManager.instance()
          .error(this, "Last %d pages in WAL segment %s are broken and will be truncate, some data will be lost after restore.",
              null, pages - currentPage - 1, path.getFileName());

      segmentCache.truncate(currentPage + 1);
      segmentCache.sync();
    }

    final int lastRecordEnd = buffer.getInt(OWALPageV2.END_LAST_RECORD);
    final int freeSpaceOffset = buffer.getInt(OWALPage.FREE_SPACE_OFFSET);

    //write to the WAL is truncated right after we write first page and going to write
    //second page all pages will be not broken but WAL itself is broken we can detect it by the fact that
    //last page is completely written but end of last record does not match the amount of free space
    if (OWALPage.PAGE_SIZE - lastRecordEnd != freeSpaceOffset) {
      OLogManager.instance().error(this, "For the page '%d' of WAL segment '%s' amount of free space '%d' does not match"
              + " the end of last record in page '%d' it will be fixed automatically but may lead to data loss during recovery after crash",
          null, currentPage, path.getFileName(), freeSpaceOffset, lastRecordEnd);
      buffer.putInt(OWALPage.FREE_SPACE_OFFSET, OWALPage.PAGE_SIZE - lastRecordEnd);
      preparePageForFlush(buffer);

      buffer.position(0);
      segmentCache.writePage(buffer, currentPage);
      segmentCache.sync();
    }

    last = new OLogSequenceNumber(order, lastPosition);
    final int freeSpace = buffer.getInt(OWALPage.FREE_SPACE_OFFSET);
    filledUpTo = currentPage * OWALPage.PAGE_SIZE + (OWALPage.PAGE_SIZE - freeSpace);
  }

  @Override
  public int compareTo(@SuppressWarnings("NullableProblems") OLogSegment other) {
    final long otherOrder = other.getOrder();

    if (order > otherOrder)
      return 1;
    else if (order < otherOrder)
      return -1;

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OLogSegmentV2 that = (OLogSegmentV2) o;

    return order == that.order;

  }

  @Override
  public int hashCode() {
    return (int) (order ^ (order >>> 32));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long filledUpTo() {
    return filledUpTo;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLogSequenceNumber begin() throws IOException {
    if (!writeCache.isEmpty())
      return new OLogSequenceNumber(order, OWALPageV2.RECORDS_OFFSET);

    if (segmentCache.filledUpTo() > 0)
      return new OLogSequenceNumber(order, OWALPageV2.RECORDS_OFFSET);

    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLogSequenceNumber end() {
    return last;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(boolean flush) throws IOException {
    close(flush);

    segmentCache.delete();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Path getPath() {
    return path;
  }

  static class OLogRecord {
    final byte[] record;
    final long   writeFrom;
    final long   writeTo;

    OLogRecord(byte[] record, long writeFrom, long writeTo) {
      this.record = record;
      this.writeFrom = writeFrom;
      this.writeTo = writeTo;
    }
  }

  static OLogRecord generateLogRecord(final long starting, final byte[] record) {
    long from = starting;
    long length = record.length;
    long resultSize;
    int freePageSpace = OWALPage.PAGE_SIZE - (int) Math.max(starting % OWALPage.PAGE_SIZE, OWALPageV2.RECORDS_OFFSET);
    int inPage = OWALPageV2.calculateRecordSize(freePageSpace);
    //the record fit in the current page
    if (inPage >= length) {
      resultSize = OWALPageV2.calculateSerializedSize((int) length);
      if (from % OWALPage.PAGE_SIZE == 0)
        from += OWALPageV2.RECORDS_OFFSET;
      return new OLogRecord(record, from, from + resultSize);
    } else {
      if (inPage > 0) {
        //space left in the current page, take it
        length -= inPage;
        resultSize = freePageSpace;
        if (from % OWALPage.PAGE_SIZE == 0)
          from += OWALPageV2.RECORDS_OFFSET;
      } else {
        //no space left, start from a new one.
        from = starting + freePageSpace + OWALPageV2.RECORDS_OFFSET;
        resultSize = -OWALPageV2.RECORDS_OFFSET;
      }

      //calculate spare page
      //add all the full pages
      resultSize += length / OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE) * OWALPage.PAGE_SIZE;

      int leftSize = (int) length % OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE);
      if (leftSize > 0) {
        //add the spare bytes at the last page
        resultSize += OWALPageV2.RECORDS_OFFSET + OWALPageV2.calculateSerializedSize(leftSize);
      }

      return new OLogRecord(record, from, from + resultSize);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLogSequenceNumber logRecord(byte[] record) {
    flushNewData = true;

    OLogRecord rec = generateLogRecord(filledUpTo, record);
    filledUpTo = rec.writeTo;
    last = new OLogSequenceNumber(order, rec.writeFrom);
    try {
      cacheLock.lock();
      writeCache.add(rec);
    } finally {
      cacheLock.unlock();

    }

    long pagesInCache = (filledUpTo - writtenUpTo) / OWALPage.PAGE_SIZE;
    if (pagesInCache > maxPagesCacheSize) {
      OLogManager.instance()
          .info(this, "Max cache limit is reached (%d vs. %d), sync write is performed", maxPagesCacheSize, pagesInCache);

      writeAheadLog.incrementCacheOverflowCount();

      try {
        commitExecutor.submit(new WriteTask()).get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw OException.wrapException(new OStorageException("Thread was interrupted during WAL write"), e);
      } catch (ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' write"), e);
      }

    }

    return last;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  public byte[] readRecord(OLogSequenceNumber lsn) throws IOException {
    final OPair<OLogSequenceNumber, byte[]> lastRecord = lastReadRecord.get();
    if (lastRecord != null && lastRecord.getKey().equals(lsn))
      return lastRecord.getValue();

    assert lsn.getSegment() == order;
    if (lsn.getPosition() >= filledUpTo)
      return null;

    if (!writeCache.isEmpty())
      flush();

    long pageIndex = lsn.getPosition() / OWALPage.PAGE_SIZE;

    byte[] record = null;
    int pageOffset = (int) (lsn.getPosition() % OWALPage.PAGE_SIZE);

    long pageCount = (filledUpTo + OWALPage.PAGE_SIZE - 1) / OWALPage.PAGE_SIZE;

    while (pageIndex < pageCount) {
      byte[] pageContent = segmentCache.readPage(pageIndex);

      if (isPageBroken(pageContent))
        throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

      final ByteBuffer buffer = ByteBuffer.wrap(pageContent).order(ByteOrder.nativeOrder());
      OWALPage page = new OWALPageV2(buffer, false);

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
        pageOffset = OWALPageV2.RECORDS_OFFSET;
        pageIndex++;
        if (pageIndex >= pageCount)
          throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");
      } else {
        if (page.getFreeSpace() >= OWALPage.MIN_RECORD_SIZE && pageIndex < pageCount - 1)
          throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

        break;
      }
    }

    lastReadRecord = new WeakReference<>(new OPair<>(lsn, record));
    return record;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException {
    final byte[] record = readRecord(lsn);
    if (record == null)
      return null;

    long pos = lsn.getPosition();
    long pageIndex = pos / OWALPage.PAGE_SIZE;
    int pageOffset = (int) (pos - pageIndex * OWALPage.PAGE_SIZE);

    int restOfRecord = record.length;
    while (restOfRecord > 0) {
      int entrySize = OWALPageV2.calculateSerializedSize(restOfRecord);
      if (entrySize + pageOffset < OWALPage.PAGE_SIZE) {
        if (entrySize + pageOffset <= OWALPage.PAGE_SIZE - OWALPage.MIN_RECORD_SIZE)
          pos += entrySize;
        else
          pos += OWALPage.PAGE_SIZE - pageOffset + OWALPageV2.RECORDS_OFFSET;
        break;
      } else if (entrySize + pageOffset == OWALPage.PAGE_SIZE) {
        pos += entrySize + OWALPageV2.RECORDS_OFFSET;
        break;
      } else {
        long chunkSize = OWALPageV2.calculateRecordSize(OWALPage.PAGE_SIZE - pageOffset);
        restOfRecord -= chunkSize;

        pos += OWALPage.PAGE_SIZE - pageOffset + OWALPageV2.RECORDS_OFFSET;
        pageOffset = OWALPageV2.RECORDS_OFFSET;
      }
    }

    if (pos >= filledUpTo)
      return null;

    return new OLogSequenceNumber(order, pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(boolean flush) throws IOException {
    if (!closed) {
      lastReadRecord.clear();

      stopBackgroundWrite(flush);

      segmentCache.close(flush);

      closed = true;

    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    writeData();
    syncData();
  }

  private void writeData() {
    if (commitExecutor.isShutdown())
      if (flushNewData)
        throw new OStorageException("Unable to write data, WAL thread is shutdown");
      else
        return;

    try {
      commitExecutor.submit(new WriteTask()).get();
    } catch (RejectedExecutionException e) {
      if (flushNewData || !commitExecutor.isShutdown()) {
        throw OException.wrapException(new OStorageException("Unable to write data"), e);
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw OException.wrapException(new OStorageException("Thread was interrupted during data write"), e);
    } catch (ExecutionException e) {
      throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' data write"), e);
    }
  }

  private void syncData() {
    if (commitExecutor.isShutdown()) {
      if (flushNewData || !Objects.equals(storedUpTo, syncedUpTo))
        throw new OStorageException("Unable to sync data, WAL thread is shutdown");
      else
        return;
    }

    try {
      commitExecutor.submit(new SyncTask()).get();
    } catch (RejectedExecutionException e) {
      if (flushNewData || !Objects.equals(storedUpTo, syncedUpTo) || !commitExecutor.isShutdown()) {
        throw OException.wrapException(new OStorageException("Unable to sync data"), e);
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw OException.wrapException(new OStorageException("Thread was interrupted during data sync"), e);
    } catch (ExecutionException e) {
      throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' data sync"), e);
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

  private boolean isPageBroken(byte[] content) {
    final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, OWALPage.MAGIC_NUMBER_OFFSET);
    if (magicNumber != OWALPageV2.MAGIC_NUMBER)
      return true;

    final CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);

    return ((int) crc32.getValue()) != OIntegerSerializer.INSTANCE.deserializeNative(content, 0);
  }

}
