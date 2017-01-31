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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

final class OLogSegment implements Comparable<OLogSegment> {
  private ODiskWriteAheadLog writeAheadLog;

  private volatile long               writtenUpTo;
  private volatile OLogSequenceNumber storedUpTo;

  private final Path path;
  private final long order;
  private final int  maxPagesCacheSize;

  private final OSegmentFile segmentFile;

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

        final long filledUpTo = segmentFile.filledUpTo();
        if (filledUpTo > curPageIndex) {
          assert filledUpTo - 1 == curPageIndex;

          buffer = segmentFile.readPageBuffer(curPageIndex);
        } else {
          buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
        }

        OLogSequenceNumber lsn;
        boolean lastToFlush = false;

        for (OLogRecord log : toFlush) {
          lsn = new OLogSequenceNumber(order, log.writeFrom);

          int pos = (int) (log.writeFrom % OWALPage.PAGE_SIZE);
          int written = 0;

          while (written < log.record.length) {
            lastToFlush = true;

            int pageFreeSpace = OWALPage.calculateRecordSize(OWALPage.PAGE_SIZE - pos);
            int contentLength = Math.min(pageFreeSpace, (log.record.length - written));
            int fromRecord = written;
            written += contentLength;

            pos = writeContentInPage(buffer, pos, log.record, written == log.record.length, fromRecord, contentLength);

            if (OWALPage.PAGE_SIZE - pos < OWALPage.MIN_RECORD_SIZE) {
              preparePageForFlush(buffer);

              segmentFile.writePage(buffer, curPageIndex);

              buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
              curPageIndex++;

              lastToFlush = false;

              pos = OWALPage.RECORDS_OFFSET;
            }
          }

          writtenUpTo = log.writeTo;
          storedUpTo = lsn;
        }

        if (lastToFlush) {
          preparePageForFlush(buffer);

          segmentFile.writePage(buffer, curPageIndex);
        }

        writeAheadLog.checkFreeSpace();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during WAL background flush", e);
      }
    }

    /**
     * Write the content in the page and return the new page cursor position.
     *
     * @param pageContent   buffer of the page to be filled
     * @param posInPage     position in the page where to write
     * @param log           content to write to the page
     * @param isLast        flag to mark if is last portion of the record
     * @param fromRecord    the start of the portion of the record to write in this page
     * @param contentLength the length of the portion of the record to write in this page
     *
     * @return the new page cursor  position after this write.
     */
    private int writeContentInPage(ByteBuffer pageContent, int posInPage, byte[] log, boolean isLast, int fromRecord,
        int contentLength) {
      pageContent.put(posInPage, !isLast ? (byte) 1 : 0);
      pageContent.put(posInPage + 1, isLast ? (byte) 1 : 0);
      pageContent.putInt(posInPage + 2, contentLength);
      pageContent.position(posInPage + OIntegerSerializer.INT_SIZE + 2);
      pageContent.put(log, fromRecord, contentLength);

      posInPage += OWALPage.calculateSerializedSize(contentLength);

      pageContent.putInt(OWALPage.FREE_SPACE_OFFSET, OWALPage.PAGE_SIZE - posInPage);
      return posInPage;
    }

    private void preparePageForFlush(ByteBuffer content) throws IOException {
      content.putLong(OWALPage.MAGIC_NUMBER_OFFSET, OWALPage.MAGIC_NUMBER);

      final byte[] data = new byte[OWALPage.PAGE_SIZE - OWALPage.MAGIC_NUMBER_OFFSET];
      content.position(OWALPage.MAGIC_NUMBER_OFFSET);
      content.get(data);

      CRC32 crc32 = new CRC32();
      crc32.update(data);
      content.putInt(OWALPage.CRC_OFFSET, (int) crc32.getValue());
    }
  }

  private final class FSyncer implements Runnable {
    @Override
    public void run() {
      try {
        fsync();
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Can not force sync content of file " + path);
      }
    }
  }

  OLogSegment(ODiskWriteAheadLog writeAheadLog, Path path, int maxPagesCacheSize, int fileTTL, int segmentBufferSize,
      ScheduledExecutorService closer, ScheduledExecutorService commitExecutor) throws IOException {
    this.writeAheadLog = writeAheadLog;
    this.path = path;
    this.maxPagesCacheSize = maxPagesCacheSize;
    this.commitExecutor = commitExecutor;

    order = extractOrder(path.getFileName().toString());

    this.segmentFile = new OSegmentFile(path, fileTTL, segmentBufferSize, closer);
    this.segmentFile.open();

    closed = false;
  }

  void startFlush() {
    if (writeAheadLog.getCommitDelay() > 0) {
      commitExecutor.scheduleAtFixedRate(new WriteTask(), 100, 100, TimeUnit.MICROSECONDS);
      commitExecutor.scheduleAtFixedRate(new FSyncer(), writeAheadLog.getCommitDelay(), writeAheadLog.getCommitDelay(),
          TimeUnit.MILLISECONDS);
    }
  }

  void stopFlush(boolean flush) throws IOException {
    if (flush)
      flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
          throw new OStorageException("WAL flush task for '" + getPath() + "' segment cannot be stopped");

      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread");
      }
    }
  }

  private void fsync() throws IOException {
    OLogSequenceNumber stored = storedUpTo;
    segmentFile.sync();

    writeAheadLog.casFlushedLsn(stored);
  }

  public long getOrder() {
    return order;
  }

  public void init() throws IOException {
    initPageCache();

    last = new OLogSequenceNumber(order, filledUpTo - 1);
  }

  @Override
  public int compareTo(OLogSegment other) {
    final long otherOrder = other.order;

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

    OLogSegment that = (OLogSegment) o;

    return order == that.order;

  }

  @Override
  public int hashCode() {
    return (int) (order ^ (order >>> 32));
  }

  public long filledUpTo() throws IOException {
    return filledUpTo;
  }

  public OLogSequenceNumber begin() throws IOException {
    if (!writeCache.isEmpty())
      return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

    if (segmentFile.filledUpTo() > 0)
      return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

    return null;
  }

  public OLogSequenceNumber end() {
    return last;
  }

  public void delete(boolean flush) throws IOException {
    close(flush);

    segmentFile.delete();
  }

  public Path getPath() {
    return path;
  }

  public static class OLogRecord {
    public final byte[] record;
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
    int freePageSpace = OWALPage.PAGE_SIZE - (int) Math.max(starting % OWALPage.PAGE_SIZE, OWALPage.RECORDS_OFFSET);
    int inPage = OWALPage.calculateRecordSize(freePageSpace);
    //the record fit in the current page
    if (inPage >= length) {
      resultSize = OWALPage.calculateSerializedSize((int) length);
      if (from % OWALPage.PAGE_SIZE == 0)
        from += OWALPage.RECORDS_OFFSET;
      return new OLogRecord(record, from, from + resultSize);
    } else {
      if (inPage > 0) {
        //space left in the current page, take it
        length -= inPage;
        resultSize = freePageSpace;
        if (from % OWALPage.PAGE_SIZE == 0)
          from += OWALPage.RECORDS_OFFSET;
      } else {
        //no space left, start from a new one.
        from = starting + freePageSpace + OWALPage.RECORDS_OFFSET;
        resultSize = -OWALPage.RECORDS_OFFSET;
      }

      //calculate spare page
      //add all the full pages
      resultSize += length / OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE) * OWALPage.PAGE_SIZE;

      int leftSize = (int) length % OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE);
      if (leftSize > 0) {
        //add the spare bytes at the last page
        resultSize += OWALPage.RECORDS_OFFSET + OWALPage.calculateSerializedSize(leftSize);
      }

      return new OLogRecord(record, from, from + resultSize);
    }
  }

  OLogSequenceNumber logRecord(byte[] record) throws IOException {
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
      byte[] pageContent = segmentFile.readPage(pageIndex);

      if (!checkPageIntegrity(pageContent))
        throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

      final ByteBuffer buffer = ByteBuffer.wrap(pageContent).order(ByteOrder.nativeOrder());
      OWALPage page = new OWALPage(buffer, false);

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
    }

    lastReadRecord = new WeakReference<>(new OPair<>(lsn, record));
    return record;
  }

  OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException {
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
        long chunkSize = OWALPage.calculateRecordSize(OWALPage.PAGE_SIZE - pageOffset);
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

      segmentFile.close(flush);

      closed = true;

    }
  }

  OLogSequenceNumber readFlushedLSN() throws IOException {
    if (segmentFile.filledUpTo() == 0)
      return null;

    return new OLogSequenceNumber(order, filledUpTo - 1);
  }

  public void flush() throws IOException {
    writeData();
    fsync();
  }

  private void writeData() {
    if (!commitExecutor.isShutdown()) {
      try {
        commitExecutor.submit(new WriteTask()).get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw OException.wrapException(new OStorageException("Thread was interrupted during flush"), e);
      } catch (ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' flush"), e);
      }
    } else {
      new WriteTask().run();
    }
  }

  private void initPageCache() throws IOException {
    long pagesCount = segmentFile.filledUpTo();
    if (pagesCount == 0)
      return;

    final byte[] content = segmentFile.readPage(pagesCount - 1);
    if (checkPageIntegrity(content)) {
      int freeSpace = OIntegerSerializer.INSTANCE.deserializeNative(content, OWALPage.FREE_SPACE_OFFSET);
      filledUpTo = (pagesCount - 1) * OWALPage.PAGE_SIZE + (OWALPage.PAGE_SIZE - freeSpace);
    } else {
      filledUpTo = pagesCount * OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET;
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

  public long getFilledUpTo() {
    return filledUpTo;
  }
}
