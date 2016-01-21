package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * Created by tglman on 20/01/16.
 */
final class OLogSegment implements Comparable<OLogSegment> {
  private final OByteBufferPool byteBufferPool = OByteBufferPool.instance();
  private       ODiskWriteAheadLog writeAheadLog;
  private final RandomAccessFile   rndFile;
  private final File               file;
  private final long               order;
  private final int                maxPagesCacheSize;
  private final AtomicReference<List<OLogRecord>> logCache       = new AtomicReference<List<OLogRecord>>(new ArrayList<OLogRecord>());
  private final ConcurrentLinkedQueue<OWALPage>   pagesCache     = new ConcurrentLinkedQueue<OWALPage>();
  private final ScheduledExecutorService          commitExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task (" + writeAheadLog.getStorage().getName() + ")");
      return thread;
    }
  });
  private volatile long     filledUpTo;
  private boolean  closed;
  private OWALPage currentPage;
  private long     nextPositionToFlush;
  private OLogSequenceNumber last = null;
  private OLogSequenceNumber pendingLSNToFlush;

  private volatile boolean flushNewData = true;

  private WeakReference<OPair<OLogSequenceNumber, byte[]>> lastReadRecord = new WeakReference<OPair<OLogSequenceNumber, byte[]>>(null);

  private final class FlushTask implements Runnable {
    private FlushTask() {
    }

    @Override
    public void run() {
      try {
        OLogSegment.this.commitLog();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during WAL background flush", e);
      }
    }
  }

  private void commitLog() throws IOException {
    if (logCache.get().isEmpty())
      return;

    if (!flushNewData)
      return;
    flushNewData = false;

    List<OLogRecord> toFlush = logCache.getAndSet(new ArrayList<OLogRecord>());
    if (toFlush.isEmpty())
      return;
    byte[] pageContent = new byte[OWALPage.PAGE_SIZE];

    OLogRecord first = toFlush.get(0);
    int curIndex = (int) (first.writeFrom / OWALPage.PAGE_SIZE);
    synchronized (rndFile) {
      long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;
      if (pagesCount > curIndex ) {
        rndFile.seek(curIndex * OWALPage.PAGE_SIZE);
        rndFile.readFully(pageContent);
      }
    }

    OLogSequenceNumber lsn = null;
    int pageIndex = 0;
    int flushedPage = curIndex;
    int pos =0;
    boolean lastToFlush=false;
    for (OLogRecord log : toFlush) {
      lsn = new OLogSequenceNumber(order, log.writeFrom);
      pos = (int) (log.writeFrom % OWALPage.PAGE_SIZE);
      pageIndex = (int) (log.writeFrom / OWALPage.PAGE_SIZE);
      int written = 0;
      boolean mergeWithNextPage = true;
      boolean recordTail = false;
      while (written < log.record.length) {
        lastToFlush =true;
        int pageFreeSpace = OWALPage.calculateRecordSize(OWALPage.PAGE_SIZE - pos);

        int contentLength = Math.min(pageFreeSpace, (log.record.length - written));
        int fromRecord = written;
        written += contentLength;
        if (written == log.record.length) {
          mergeWithNextPage = false;
          recordTail = true;
        }
        OByteSerializer.INSTANCE.serializeNative(mergeWithNextPage ? (byte) 1 : 0, pageContent, pos);
        OByteSerializer.INSTANCE.serializeNative(recordTail ? (byte) 1 : 0, pageContent, pos + 1);
        OIntegerSerializer.INSTANCE.serializeNative(contentLength, pageContent, pos + 2);
        System.arraycopy(log.record, fromRecord, pageContent, pos + OIntegerSerializer.INT_SIZE + 2, contentLength);
        pos += OWALPage.calculateSerializedSize(contentLength);
        OIntegerSerializer.INSTANCE.serializeNative(OWALPage.PAGE_SIZE - pos, pageContent, OWALPage.FREE_SPACE_OFFSET);
        if (OWALPage.PAGE_SIZE - pos < OWALPage.MIN_RECORD_SIZE) {
          synchronized (rndFile) {
            OLongSerializer.INSTANCE.serializeNative(OWALPage.MAGIC_NUMBER, pageContent, OWALPage.MAGIC_NUMBER_OFFSET);
            rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
            flushPage(pageContent);
          }
          if (pendingLSNToFlush != null) {
            this.writeAheadLog.setFlushedLsn(pendingLSNToFlush);
          }
          pendingLSNToFlush = lsn;
          lastToFlush=false;
          pageIndex++;
          pos = OWALPage.RECORDS_OFFSET;
        }
      }

    }
    if(lastToFlush) {
      synchronized (rndFile) {
        OLongSerializer.INSTANCE.serializeNative(OWALPage.MAGIC_NUMBER, pageContent, OWALPage.MAGIC_NUMBER_OFFSET);
        rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
        flushPage(pageContent);
      }
    }
    this.writeAheadLog.setFlushedLsn(lsn);

  }

  private void commit() throws IOException {
    if (pagesCache.isEmpty())
      return;

    if (!flushNewData)
      return;

    flushNewData = false;

    final int maxSize = pagesCache.size();

    ByteBuffer[] pagesToFlush = new ByteBuffer[maxSize];

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
              lastLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * (long) OWALPage.PAGE_SIZE + pos);
          } else if (pendingLSNToFlush == null)
            pendingLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * (long) OWALPage.PAGE_SIZE + pos);

          pos += page.getSerializedRecordSize(pos);
        }

        final ByteBuffer dataBuffer;

        if (flushedPages == maxSize - 1) {
          dataBuffer = byteBufferPool.acquireDirect(false);

          final ByteBuffer pageBuffer = page.getByteBuffer();

          pageBuffer.position(0);
          dataBuffer.position(0);

          dataBuffer.put(pageBuffer);
        } else {
          ByteBuffer buffer = page.getByteBuffer();
          dataBuffer = buffer.duplicate();
          dataBuffer.order(buffer.order());
        }

        pagesToFlush[flushedPages] = dataBuffer;
      }

      flushedPages++;
    }

    synchronized (rndFile) {
      rndFile.seek(filePointer);
      for (int i = 0; i < pagesToFlush.length; i++) {
        final ByteBuffer dataBuffer = pagesToFlush[i];
        byte[] pageContent = new byte[OWALPage.PAGE_SIZE];
        dataBuffer.position(0);
        dataBuffer.get(pageContent);

        if (i == pagesToFlush.length - 1)
          byteBufferPool.release(dataBuffer);

        flushPage(pageContent);
        filePointer += OWALPage.PAGE_SIZE;
      }

      if (OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.getValueAsBoolean())
        rndFile.getFD().sync();
    }

    nextPositionToFlush = filePointer - OWALPage.PAGE_SIZE;

    if (lastLSNToFlush != null)
      writeAheadLog.setFlushedLsn(lastLSNToFlush);

    for (int i = 0; i < flushedPages - 1; i++) {
      OWALPage page = pagesCache.poll();
      byteBufferPool.release(page.getByteBuffer());
    }

    assert !pagesCache.isEmpty();

    writeAheadLog.checkFreeSpace();
  }

  private void flushPage(byte[] content) throws IOException {
    CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), content, 0);

    rndFile.write(content);
  }


  OLogSegment(ODiskWriteAheadLog writeAheadLog, File file, int maxPagesCacheSize) throws IOException {
    this.writeAheadLog = writeAheadLog;
    this.file = file;
    this.maxPagesCacheSize = maxPagesCacheSize;

    order = extractOrder(file.getName());
    closed = false;
    rndFile = new RandomAccessFile(file, "rw");
  }

  public void startFlush() {
    if (writeAheadLog.getCommitDelay() > 0)
      commitExecutor.scheduleAtFixedRate(new FlushTask(), writeAheadLog.getCommitDelay(), writeAheadLog.getCommitDelay(), TimeUnit.MILLISECONDS);
  }

  public void stopFlush(boolean flush) {
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

  public long getOrder() {
    return order;
  }

  public void init() throws IOException {
    selfCheck();

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
    if (!logCache.get().isEmpty())
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

  public static class OLogRecord {
    public final byte[] record;
    public final long   writeFrom;
    public final long   writeTo;

    public OLogRecord(byte[] record, long writeFrom, long writeTo) {
      this.record = record;
      this.writeFrom = writeFrom;
      this.writeTo = writeTo;
    }
  }

  public static OLogRecord generateLogRecord(final long starting, final byte[] record) {
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

  public OLogSequenceNumber logRecord(byte[] record) throws IOException {
    flushNewData = true;

    OLogRecord rec = generateLogRecord(filledUpTo, record);
    filledUpTo = rec.writeTo;
    last = new OLogSequenceNumber(order, rec.writeFrom);
    logCache.get().add(rec);
    long flushedPos = 0;
    if (writeAheadLog.getFlushedLsn() != null)
      flushedPos = writeAheadLog.getFlushedLsn().getPosition();
    if ((filledUpTo - flushedPos) / OWALPage.PAGE_SIZE > maxPagesCacheSize) {
      OLogManager.instance().info(this, "Max cache limit is reached (%d vs. %d), sync flush is performed", maxPagesCacheSize, pagesCache.size());
      flush();
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

    if (!logCache.get().isEmpty())
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

      final ByteBuffer buffer = byteBufferPool.acquireDirect(false);
      buffer.put(pageContent);
      try {
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
      } finally {
        byteBufferPool.release(buffer);
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

      rndFile.close();

      closed = true;

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
        throw OException.wrapException(new OStorageException("Thread was interrupted during flush"), e);
      } catch (ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' flush"), e);
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
        final ByteBuffer pageBuffer = byteBufferPool.acquireDirect(false);
        pageBuffer.put(content);
        currentPage = new OWALPage(pageBuffer, false);
        filledUpTo = (pagesCount - 1) * OWALPage.PAGE_SIZE + currentPage.getFilledUpTo();
        nextPositionToFlush = (pagesCount - 1) * OWALPage.PAGE_SIZE;
      } else {
        final ByteBuffer pageBuffer = byteBufferPool.acquireDirect(false);
        currentPage = new OWALPage(pageBuffer, true);
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
    if (!logCache.get().isEmpty())
      throw new IllegalStateException("WAL cache is not empty, we cannot verify WAL after it was started to be used");

    synchronized (rndFile) {
      long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;

      if (rndFile.length() % OWALPage.PAGE_SIZE > 0) {
        OLogManager.instance().error(this, "Last WAL page was written partially, auto fix");

        rndFile.setLength(OWALPage.PAGE_SIZE * pagesCount);
      }
    }
  }

  public long getFilledUpTo() {
    return filledUpTo;
  }
}
