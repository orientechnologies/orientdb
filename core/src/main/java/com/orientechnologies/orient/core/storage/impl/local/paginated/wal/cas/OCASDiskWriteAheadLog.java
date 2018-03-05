package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class OCASDiskWriteAheadLog {
  public static final String WAL_SEGMENT_EXTENSION = ".wal";
  private final       int    BATCH_READ_SIZE       = 320;

  private final long walSizeHardLimit = OGlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * 1024 * 1024;
  private       long walSizeLimit     = walSizeHardLimit;

  private final int maxSegmentSize;

  private final    LongAdder ongoingTXs     = new LongAdder();
  private final    long      freeSpaceLimit = OGlobalConfiguration.DISK_CACHE_FREE_SPACE_LIMIT.getValueAsLong() * 1024 * 1024;
  private volatile long      freeSpace      = -1;

  private final ConcurrentLinkedDeque<OWALRecord> records        = new ConcurrentLinkedDeque<>();
  private final AtomicLong                        currentSegment = new AtomicLong();

  private final AtomicLong segmentSize = new AtomicLong();
  private final AtomicLong logSize     = new AtomicLong();
  private final AtomicLong queueSize   = new AtomicLong();

  private final int maxCacheSize;

  private final AtomicReference<OLogSequenceNumber> end      = new AtomicReference<>();
  private final ConcurrentSkipListSet<Long>         segments = new ConcurrentSkipListSet<>();

  private final OScheduledThreadPoolExecutorWithLogging commitExecutor;

  private final FileStore fileStore;
  private final Path      walLocation;
  private final String    storageName;

  private volatile FileChannel walChannel = null;

  private volatile OLogSequenceNumber flushedLSN = null;

  private long segmentId = -1;

  private final ConcurrentSkipListMap<OLogSequenceNumber, Integer> cutTillLimits = new ConcurrentSkipListMap<>();

  private final ScheduledFuture<?> recordsWriterFuture;

  public OCASDiskWriteAheadLog(String storageName, Path storagePath, final Path walPath, int maxPagesCacheSize, int maxSegmentSize,
      int commitDelay, boolean filterWALFiles, Locale locale) throws IOException {
    commitExecutor = new OScheduledThreadPoolExecutorWithLogging(1, r -> {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task (" + storageName + ")");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    });

    commitExecutor.setMaximumPoolSize(1);

    this.walLocation = calculateWalPath(storagePath, walPath);
    if (!Files.exists(walLocation)) {
      Files.createDirectories(walLocation);
    }

    this.fileStore = Files.getFileStore(walLocation);
    this.storageName = storageName;
    this.maxCacheSize = maxPagesCacheSize * OCASWALPage.PAGE_SIZE;

    logSize.set(initSegmentSet(filterWALFiles, locale));

    long nextSegmentId;

    if (segments.isEmpty()) {
      nextSegmentId = 1;
    } else {
      nextSegmentId = segments.last() + 1;
    }

    currentSegment.set(nextSegmentId);
    this.maxSegmentSize = maxSegmentSize;

    //we log empty record on open so end of WAL will always contain valid value
    final OStartWALRecord startRecord = new OStartWALRecord();

    startRecord.setLsn(new OLogSequenceNumber(currentSegment.get(), OCASWALPage.RECORDS_OFFSET));
    startRecord.setDistance(0);
    startRecord.setDiskSize(0);

    records.add(startRecord);

    log(new OEmptyWALRecord());

    this.recordsWriterFuture = commitExecutor
        .scheduleAtFixedRate(new RecordsWriter(), commitDelay, commitDelay, TimeUnit.MILLISECONDS);
  }

  private long initSegmentSet(boolean filterWALFiles, Locale locale) throws IOException {
    Stream<Path> walFiles;

    OModifiableLong walSize = new OModifiableLong();
    if (filterWALFiles)
      walFiles = Files.find(walLocation, 1,
          (Path path, BasicFileAttributes attributes) -> validateName(path.getFileName().toString(), storageName, locale));
    else
      walFiles = Files.find(walLocation, 1,
          (Path path, BasicFileAttributes attrs) -> validateSimpleName(path.getFileName().toString(), locale));

    if (walFiles == null)
      throw new IllegalStateException(
          "Location passed in WAL does not exist, or IO error was happened. DB cannot work in durable mode in such case");

    walFiles.forEach((Path path) -> {
      try {
        walSize.increment(Files.size(path));
        segments.add(extractSegmentId(path.getFileName().toString()));
      } catch (IOException e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL loading", e);
        throw new IllegalStateException("Error during WAL loading", e);
      }
    });

    return walSize.value;
  }

  private long extractSegmentId(String name) {
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

  private static boolean validateName(String name, String storageName, Locale locale) {
    name = name.toLowerCase(locale);
    storageName = storageName.toLowerCase(locale);

    if (!name.endsWith(".wal"))
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
    } catch (NumberFormatException ignore) {
      return false;
    }

    return true;
  }

  private static boolean validateSimpleName(String name, Locale locale) {
    name = name.toLowerCase(locale);

    if (!name.endsWith(".wal"))
      return false;

    int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      //noinspection ResultOfMethodCallIgnored
      Integer.parseInt(walOrder);
    } catch (NumberFormatException ignore) {
      return false;
    }

    return true;
  }

  private Path calculateWalPath(Path storagePath, Path walPath) {
    if (walPath == null)
      return storagePath;

    return walPath;
  }

  public List<OWriteableWALRecord> read(final OLogSequenceNumber lsn, int limit) throws IOException {
    final OLogSequenceNumber endLSN = end.get();

    if (lsn.compareTo(endLSN) > 0) {
      return Collections.emptyList();
    }

    addCutTillLimit(lsn);
    try {
      Iterator<OWALRecord> recordIterator = records.iterator();

      OWALRecord record = recordIterator.next();
      OLogSequenceNumber logRecordLSN = record.getLsn();

      while (logRecordLSN.getPosition() > 0 && logRecordLSN.compareTo(lsn) <= 0) {
        while (true) {
          final int compare = logRecordLSN.compareTo(lsn);

          if (compare == 0 && record instanceof OWriteableWALRecord) {
            return Collections.singletonList((OWriteableWALRecord) record);
          }

          if (compare > 0) {
            return Collections.emptyList();
          }

          if (recordIterator.hasNext()) {
            record = recordIterator.next();
            logRecordLSN = record.getLsn();

            if (logRecordLSN.getPosition() < 0) {
              return Collections.emptyList();
            }
          } else {
            recordIterator = records.iterator();
            record = recordIterator.next();
            break;
          }
        }
      }

      final List<OWriteableWALRecord> result = new ArrayList<>();
      long position = lsn.getPosition();
      long pageIndex = position / OCASWALPage.PAGE_SIZE;
      long segment = lsn.getSegment();

      int pagesRead = 0;

      Iterator<Long> segmentsIterator = segments.tailSet(segment).iterator();

      while (pagesRead < BATCH_READ_SIZE) {
        if (segmentsIterator.hasNext()) {
          byte[] recordContent = null;
          int recordLen = -1;

          byte[] recordLenBytes = null;
          int recordLenRead = -1;

          int bytesRead = 0;

          long lsnPos = -1;

          segment = segmentsIterator.next();

          final String segmentName = getSegmentName(segment);
          final Path segmentPath = walLocation.resolve(segmentName);

          try (FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
            final long chSize = channel.size();

            while (pageIndex * OCASWALPage.PAGE_SIZE < chSize) {
              channel.position(pageIndex * OCASWALPage.PAGE_SIZE);

              final long ptr = Native.malloc(OCASWALPage.PAGE_SIZE);
              try {
                ByteBuffer buffer = new Pointer(ptr).getByteBuffer(0, OCASWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
                OIOUtils.readByteBuffer(buffer, channel);
                pagesRead++;

                if (pageIsBroken(buffer)) {
                  OLogManager.instance().errorNoDb(this, "WAL page %d is broken, read of records will be stopped", null, pageIndex);
                  return result;
                }

                buffer.position((int) (position - pageIndex * OCASWALPage.PAGE_SIZE));

                while (buffer.position() < OCASWALPage.PAGE_SIZE) {
                  if (recordLen == -1) {
                    if (recordLenBytes == null) {
                      lsnPos = pageIndex * OCASWALPage.PAGE_SIZE + buffer.position();

                      if (buffer.remaining() >= OIntegerSerializer.INT_SIZE) {
                        recordLen = buffer.getInt();
                      } else {

                        final byte stopFlag = buffer.get(OCASWALPage.STOP_PAGE_OFFSET);

                        if (stopFlag == 1) {
                          buffer.position(OCASWALPage.PAGE_SIZE);
                          continue;
                        } else if (stopFlag != 0) {
                          throw new IllegalStateException("Invalid value of stop flag " + stopFlag);
                        }

                        recordLenBytes = new byte[OIntegerSerializer.INT_SIZE];
                        recordLenRead = buffer.remaining();

                        buffer.get(recordLenBytes, 0, recordLenRead);
                        continue;
                      }
                    } else {
                      if (recordLenRead < OIntegerSerializer.INT_SIZE) {
                        buffer.get(recordLenBytes, recordLenRead, OIntegerSerializer.INT_SIZE - recordLenRead);
                      }

                      recordLen = OIntegerSerializer.INSTANCE.deserializeNative(recordLenBytes, 0);
                    }

                    if (recordLen == 0) {
                      //end of page is reached
                      recordLen = -1;
                      recordLenBytes = null;
                      recordLenRead = -1;

                      break;
                    }

                    recordContent = new byte[recordLen];
                  }

                  final int bytesToRead = Math.min(recordLen - bytesRead, buffer.remaining());
                  buffer.get(recordContent, bytesRead, bytesToRead);
                  bytesRead += bytesToRead;

                  if (bytesRead == recordLen) {
                    final OWriteableWALRecord walRecord = OWALRecordsFactory.INSTANCE.fromStream(recordContent);
                    walRecord.setLsn(new OLogSequenceNumber(segment, lsnPos));

                    recordContent = null;
                    bytesRead = 0;
                    recordLen = -1;

                    recordLenBytes = null;
                    recordLenRead = -1;

                    result.add(walRecord);

                    if (result.size() == limit) {
                      return result;
                    }
                  }
                }
              } finally {
                Native.free(ptr);
              }

              pageIndex++;
              position = pageIndex * OCASWALPage.PAGE_SIZE + OCASWALPage.RECORDS_OFFSET;
            }
          }

          pageIndex = 0;
          position = OCASWALPage.RECORDS_OFFSET;
        } else {
          break;
        }
      }

      return result;
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  public List<OWriteableWALRecord> next(final OLogSequenceNumber lsn) throws IOException {
    addCutTillLimit(lsn);
    try {
      final OLogSequenceNumber endLSN = end.get();
      final int compareEnd = lsn.compareTo(endLSN);

      if (compareEnd == 0) {
        return Collections.emptyList();
      }

      if (compareEnd > 0) {
        throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
      }

      Iterator<OWALRecord> recordIterator = records.iterator();

      OWALRecord logRecord = recordIterator.next();
      OLogSequenceNumber logRecordLSN = logRecord.getLsn();

      while (logRecord.getLsn().getPosition() > 0 && logRecordLSN.compareTo(lsn) <= 0) {
        while (true) {
          final int compare = logRecordLSN.compareTo(lsn);

          if (compare == 0) {
            while (recordIterator.hasNext()) {
              OWALRecord nextRecord = recordIterator.next();

              if (nextRecord instanceof OWriteableWALRecord) {
                return Collections.singletonList((OWriteableWALRecord) nextRecord);
              }
            }

            break;
          } else if (compare < 0) {
            if (recordIterator.hasNext()) {
              logRecord = recordIterator.next();
              logRecordLSN = logRecord.getLsn();

              if (logRecordLSN.getPosition() < 0) {
                return Collections.emptyList();
              }
            } else {
              recordIterator = records.iterator();
              logRecord = recordIterator.next();
              break;
            }
          } else {
            throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
          }
        }
      }

      final List<OWriteableWALRecord> result = new ArrayList<>();
      long position = lsn.getPosition();
      long pageIndex = position / OCASWALPage.PAGE_SIZE;
      long segment = lsn.getSegment();

      byte[] recordContent = null;
      int recordLen = -1;
      int bytesRead = 0;
      int pagesRead = 0;

      boolean initialRecord = true;

      Iterator<Long> segmentsIterator = segments.tailSet(segment).iterator();

      while (pagesRead < BATCH_READ_SIZE) {
        if (!segmentsIterator.hasNext()) {
          if (initialRecord) {
            throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
          }

          return result;
        } else {
          segment = segmentsIterator.next();

          final String segmentName = getSegmentName(segment);
          final Path segmentPath = walLocation.resolve(segmentName);

          try (FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
            if (initialRecord && pageIndex * OCASWALPage.PAGE_SIZE < channel.size()) {
              throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
            }

            while (pageIndex * OCASWALPage.PAGE_SIZE < channel.size()) {
              channel.position(pageIndex * OCASWALPage.PAGE_SIZE);

              long ptr = Native.malloc(OCASWALPage.PAGE_SIZE);
              try {
                ByteBuffer buffer = new Pointer(ptr).getByteBuffer(0, OCASWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
                OIOUtils.readByteBuffer(buffer, channel);
                pagesRead++;

                if (pageIsBroken(buffer)) {
                  OLogManager.instance().errorNoDb(this, "WAL page %d is broken, read of records will be stopped", null, pageIndex);
                  return result;
                }

                buffer.position((int) (position - pageIndex * OCASWALPage.PAGE_SIZE));

                if (initialRecord && buffer.position() >= OCASWALPage.PAGE_SIZE) {
                  throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
                }

                while (buffer.position() < OCASWALPage.PAGE_SIZE) {
                  if (recordLen == -1) {
                    recordLen = buffer.getInt();

                    if (initialRecord) {
                      initialRecord = false;

                      final int spaceLeft = OCASWALPage.PAGE_SIZE - buffer.position() + OIntegerSerializer.INT_SIZE;
                      int length = OCASWALPage.calculateSerializedSize(recordLen);

                      if (length <= spaceLeft) {
                        position += length;
                      } else {
                        length -= spaceLeft;

                        final int begining = spaceLeft;
                        final int pages = length / OCASWALPage.MAX_RECORD_SIZE;
                        final int offset = length - pages * OCASWALPage.PAGE_SIZE;

                        if (offset == 0) {
                          final int distance = begining + pages * OCASWALPage.PAGE_SIZE;
                          position += distance;
                        } else {
                          final int distance = begining + pages * OCASWALPage.PAGE_SIZE + offset + OCASWALPage.RECORDS_OFFSET;
                          position += distance;
                        }

                        pageIndex = position / OCASWALPage.PAGE_SIZE;

                        recordLen = -1;
                        bytesRead = 0;

                        break;
                      }
                    }

                    recordContent = new byte[recordLen];

                  }

                  final int bytesToRead = Math.min(recordLen - bytesRead, buffer.remaining());
                  buffer.get(recordContent, bytesRead, bytesToRead);
                  bytesRead += bytesToRead;

                  if (bytesRead == recordLen) {
                    final OWriteableWALRecord walRecord = OWALRecordsFactory.INSTANCE.fromStream(recordContent);

                    recordContent = null;
                    bytesRead = 0;
                    recordLen = -1;

                    result.add(walRecord);
                  }
                }
              } finally {
                Native.free(ptr);
              }

              pageIndex++;
              position = pageIndex * OCASWALPage.PAGE_SIZE + OCASWALPage.RECORDS_OFFSET;
            }
          }
        }
      }

      return result;
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  private boolean pageIsBroken(ByteBuffer buffer) {
    buffer.position(OCASWALPage.MAGIC_NUMBER_OFFSET);

    if (buffer.getLong() != OCASWALPage.MAGIC_NUMBER) {
      return true;
    }

    CRC32 crc32 = new CRC32();
    buffer.position(OCASWALPage.RECORDS_OFFSET);

    crc32.update(buffer);
    buffer.position(OCASWALPage.CRC32_OFFSET);
    if (((int) crc32.getValue()) != buffer.getInt()) {
      return true;
    }

    return false;
  }

  public void addCutTillLimit(OLogSequenceNumber lsn) {
    if (lsn == null)
      throw new NullPointerException();

    while (true) {
      final Integer oldCounter = cutTillLimits.get(lsn);

      final Integer newCounter;

      if (oldCounter == null) {
        if (cutTillLimits.putIfAbsent(lsn, 1) == null)
          break;
      } else {
        newCounter = oldCounter + 1;

        if (cutTillLimits.replace(lsn, oldCounter, newCounter)) {
          break;
        }
      }
    }
  }

  public void removeCutTillLimit(OLogSequenceNumber lsn) {
    if (lsn == null)
      throw new NullPointerException();

    while (true) {
      final Integer oldCounter = cutTillLimits.get(lsn);

      if (oldCounter == null)
        throw new IllegalArgumentException(String.format("Limit %s is going to be removed but it was not added", lsn));

      final Integer newCounter = oldCounter - 1;
      if (cutTillLimits.replace(lsn, oldCounter, newCounter)) {
        if (newCounter == 0) {
          cutTillLimits.remove(lsn, newCounter);
        }

        break;
      }
    }
  }

  public OLogSequenceNumber log(OWriteableWALRecord writeableRecord) {
    if (writeableRecord instanceof OAtomicUnitStartRecord) {
      ongoingTXs.increment();
    }

    final OLogSequenceNumber recordLSN = doLogRecord(writeableRecord);

    final long qsize = queueSize.getAndAdd(writeableRecord.getBinaryContent().length);
    if (qsize >= maxCacheSize) {
      doFlush();
    }

    final int diskSize = writeableRecord.getDiskSize();
    final long size = logSize.addAndGet(diskSize);
    final long segSize = segmentSize.addAndGet(diskSize);

    if (writeableRecord instanceof OAtomicUnitEndRecord) {
      ongoingTXs.decrement();
    }

    return recordLSN;
  }

  public void appendNewSegment() {
    if (ongoingTXs.sum() > 0) {
      throw new IllegalStateException("There are on going txs, such call can be dangerous and unpredictable");
    }

    currentSegment.incrementAndGet();

    logMilestoneRecord();
  }

  private OLogSequenceNumber doLogRecord(OWriteableWALRecord writeableRecord) {
    writeableRecord.setBinaryContent(OWALRecordsFactory.INSTANCE.toStream(writeableRecord));
    writeableRecord.setLsn(new OLogSequenceNumber(currentSegment.get(), -1));

    records.add(writeableRecord);

    calculateRecordsLSNs();

    final OLogSequenceNumber recordLSN = writeableRecord.getLsn();

    OLogSequenceNumber endLsn = end.get();
    while (endLsn == null || recordLSN.compareTo(endLsn) > 0) {
      if (end.compareAndSet(endLsn, recordLSN)) {
        break;
      }

      endLsn = end.get();
    }

    return recordLSN;
  }

  public void flush() {
    doFlush();
  }

  public void close() throws IOException {
    close(true);
  }

  public void close(boolean flush) throws IOException {
    if (flush) {
      doFlush();
    }

    commitExecutor.shutdown();
    try {
      if (!commitExecutor.awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
        throw new OStorageException("WAL flush task for '" + storageName + "' storage cannot be stopped");

    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread", e);
    }

    walChannel.close();
    cutTillLimits.clear();

    if (!recordsWriterFuture.isCancelled()) {
      try {
        recordsWriterFuture.get(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS);
      } catch (CancellationException e) {
        //ignore
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread", e);
      } catch (ExecutionException | TimeoutException e) {
        throw new OStorageException("WAL flush task for '" + storageName + "' storage cannot be stopped");
      }
    }
  }

  private void checkFreeSpace() throws IOException {
    freeSpace = fileStore.getUsableSpace();
  }

  private void doFlush() {
    final Future<?> future = commitExecutor.submit(new RecordsWriter());
    try {
      future.get();
    } catch (Exception e) {
      OLogManager.instance().errorNoDb(this, "Exception during WAL flush", e);
      throw new IllegalStateException(e);
    }
  }

  public OLogSequenceNumber getFlushedLSN() {
    return flushedLSN;
  }

  private void calculateRecordsLSNs() {
    final Iterator<OWALRecord> iterator = records.descendingIterator();
    final List<OWALRecord> unassignedList = new ArrayList<>();

    while (iterator.hasNext()) {
      final OWALRecord record = iterator.next();
      final OLogSequenceNumber lsn = record.getLsn();

      if (lsn.getPosition() == -1) {
        unassignedList.add(record);
      } else {
        unassignedList.add(record);
        break;
      }
    }

    if (!unassignedList.isEmpty()) {
      final ListIterator<OWALRecord> unassignedRecordsIterator = unassignedList.listIterator(unassignedList.size());

      OWALRecord prevRecord = unassignedRecordsIterator.previous();
      OLogSequenceNumber prevLSN = prevRecord.getLsn();

      assert prevLSN.getPosition() >= 0;

      while (unassignedRecordsIterator.hasPrevious()) {
        OWALRecord record = unassignedRecordsIterator.previous();
        OLogSequenceNumber lsn = record.getLsn();

        if (lsn.getPosition() < 0) {
          final long position = calculatePosition(record, prevRecord);
          final OLogSequenceNumber newLSN = new OLogSequenceNumber(lsn.getSegment(), position);

          if (record.getLsn().getPosition() < 0) {
            record.casLSN(lsn, newLSN);
          }
        }

        prevRecord = record;
      }
    }
  }

  private OMilestoneWALRecord logMilestoneRecord() {
    final OMilestoneWALRecord milestoneRecord = new OMilestoneWALRecord();
    milestoneRecord.setLsn(new OLogSequenceNumber(currentSegment.get(), -1));

    records.add(milestoneRecord);

    calculateRecordsLSNs();

    return milestoneRecord;
  }

  public OLogSequenceNumber end() {
    return end.get();
  }

  private long calculatePosition(OWALRecord record, OWALRecord prevRecord) {
    final long prevStart;

    if (record.getLsn().getSegment() == prevRecord.getLsn().getSegment()) {
      prevStart = prevRecord.getLsn().getPosition();
    } else {
      prevStart = OCASWALPage.RECORDS_OFFSET;
    }

    assert prevRecord.getLsn().getSegment() <= record.getLsn().getSegment();

    if (prevRecord instanceof OStartWALRecord) {
      assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();

      if (record instanceof OMilestoneWALRecord) {
        record.setDistance(0);
        record.setDiskSize(0);
      } else {
        final int recordLength = ((OWriteableWALRecord) record).getBinaryContent().length;
        final int length = OCASWALPage.calculateSerializedSize(recordLength);

        final int pages = length / OCASWALPage.MAX_RECORD_SIZE;
        final int offset = length - pages * OCASWALPage.MAX_RECORD_SIZE;

        final int distance;
        if (pages == 0) {
          distance = length;
        } else {
          distance = (pages - 1) * OCASWALPage.PAGE_SIZE + offset + OCASWALPage.MAX_RECORD_SIZE + OCASWALPage.RECORDS_OFFSET;
        }

        record.setDistance(distance);
        record.setDiskSize(distance + OCASWALPage.RECORDS_OFFSET);
      }

      return prevStart;
    }

    if (prevRecord instanceof OMilestoneWALRecord) {
      if (record instanceof OMilestoneWALRecord) {
        record.setDistance(0);
        //repeat previous record disk size so it will be used in first writable record
        if (prevRecord.getLsn().getSegment() == record.getLsn().getSegment()) {
          record.setDiskSize(prevRecord.getDiskSize());
        } else {
          record.setDiskSize(0);
        }
      } else {
        //we always start from the begging of the page so no need to calculate page offset
        //record is written from the begging of page
        final int recordLength = ((OWriteableWALRecord) record).getBinaryContent().length;
        final int length = OCASWALPage.calculateSerializedSize(recordLength);

        final int pages = length / OCASWALPage.MAX_RECORD_SIZE;
        final int offset = length - pages * OCASWALPage.MAX_RECORD_SIZE;

        final int distance;
        if (pages == 0) {
          distance = length;
        } else {
          distance = (pages - 1) * OCASWALPage.PAGE_SIZE + offset + OCASWALPage.MAX_RECORD_SIZE + OCASWALPage.RECORDS_OFFSET;
        }

        record.setDistance(distance);

        final int disksize;

        if (offset == 0) {
          disksize = distance - OCASWALPage.RECORDS_OFFSET;
        } else {
          disksize = distance;
        }

        if (prevRecord.getLsn().getSegment() == record.getLsn().getSegment()) {
          record.setDiskSize(disksize + prevRecord.getDiskSize());
        } else {
          record.setDiskSize(disksize);
        }
      }

      return prevStart;
    }

    if (record instanceof OMilestoneWALRecord) {
      final long end = prevStart + prevRecord.getDistance();
      final long pageIndex = end / OCASWALPage.PAGE_SIZE;

      final long newPosition;
      final int pageOffset = (int) (end - pageIndex * OCASWALPage.PAGE_SIZE);
      if (pageOffset > OCASWALPage.RECORDS_OFFSET) {
        newPosition = (pageIndex + 1) * OCASWALPage.PAGE_SIZE + OCASWALPage.RECORDS_OFFSET;
        record.setDiskSize((int) ((pageIndex + 1) * OCASWALPage.PAGE_SIZE - end));
      } else {
        newPosition = end;
        record.setDiskSize(0);
      }

      record.setDistance(0);

      return newPosition;
    }

    final long start = prevRecord.getDistance() + prevStart;
    final int freeSpace = OCASWALPage.PAGE_SIZE - (int) (start % OCASWALPage.PAGE_SIZE);
    final int startOffset = OCASWALPage.PAGE_SIZE - freeSpace;

    final int recordLength = ((OWriteableWALRecord) record).getBinaryContent().length;
    int length = OCASWALPage.calculateSerializedSize(recordLength);

    if (length < freeSpace) {
      record.setDistance(length);

      if (startOffset == OCASWALPage.RECORDS_OFFSET) {
        record.setDiskSize(length + OCASWALPage.RECORDS_OFFSET);
      } else {
        record.setDiskSize(length);
      }

    } else {
      length -= freeSpace;

      final int firstChunk = freeSpace;
      final int pages = length / OCASWALPage.MAX_RECORD_SIZE;
      final int offset = length - pages * OCASWALPage.MAX_RECORD_SIZE;

      final int distance = firstChunk + pages * OCASWALPage.PAGE_SIZE + offset + OCASWALPage.RECORDS_OFFSET;
      record.setDistance(distance);

      int diskSize = distance;

      if (offset == 0) {
        diskSize -= OCASWALPage.RECORDS_OFFSET;
      }

      record.setDiskSize(diskSize);
    }

    return start;
  }

  private String getSegmentName(long segment) {
    return storageName + "." + segment + WAL_SEGMENT_EXTENSION;
  }

  private final class RecordsWriter implements Runnable {

    @Override
    public void run() {
      try {
        if (!checkPresenceWritableRecords()) {
          return;
        }

        final OMilestoneWALRecord milestoneWALRecord = logMilestoneRecord();
        final Iterator<OWALRecord> recordIterator = records.iterator();

        assert recordIterator.hasNext();

        OWALRecord record = recordIterator.next();
        assert record instanceof OMilestoneWALRecord || record instanceof OStartWALRecord;
        recordIterator.remove();

        long ptr = -1;
        ByteBuffer buffer = null;

        OLogSequenceNumber lastLSN = null;

        while (true) {
          record = recordIterator.next();

          if (record == milestoneWALRecord) {
            break;
          }

          assert record instanceof OWriteableWALRecord;

          final OWriteableWALRecord writeableRecord = (OWriteableWALRecord) record;
          final OLogSequenceNumber lsn = writeableRecord.getLsn();

          assert lsn.getSegment() >= segmentId;

          if (segmentId != lsn.getSegment()) {
            if (walChannel != null) {
              walChannel.close();
            }

            segmentId = lsn.getSegment();
            walChannel = FileChannel
                .open(walLocation.resolve(getSegmentName(segmentId)), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            segments.add(segmentId);

            assert lsn.getPosition() == OCASWALPage.RECORDS_OFFSET;
          }

          final byte[] recordContent = writeableRecord.getBinaryContent();
          assert recordContent != null;

          int written = 0;
          int bytesToWrite = OIntegerSerializer.INT_SIZE + recordContent.length;

          byte[] recordSize = null;
          int recordSizeWritten = -1;

          boolean recordSizeIsWritten = false;

          while (written < bytesToWrite) {
            if (buffer == null || buffer.position() == OCASWALPage.PAGE_SIZE) {
              if (ptr > 0) {
                writeBuffer(walChannel, buffer);
                Native.free(ptr);
              }

              ptr = Native.malloc(OCASWALPage.PAGE_SIZE);

              final Pointer pointer = new Pointer(ptr);
              pointer.setMemory(0, OCASWALPage.PAGE_SIZE, (byte) 0);

              buffer = pointer.getByteBuffer(0, OCASWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());
              buffer.position(OCASWALPage.RECORDS_OFFSET);
            }

            if (written == 0) {
              assert walChannel.position() + buffer.position() == lsn.getPosition();
            }

            final int chunkSize = Math.min(bytesToWrite - written, buffer.remaining());

            if (!recordSizeIsWritten) {
              if (OIntegerSerializer.INT_SIZE <= chunkSize) {
                if (recordSizeWritten < 0) {
                  buffer.putInt(recordContent.length);
                  written += OIntegerSerializer.INT_SIZE;

                  recordSizeIsWritten = true;
                  continue;
                } else {
                  buffer.put(recordSize, recordSizeWritten, OIntegerSerializer.INT_SIZE - recordSizeWritten);
                  written += OIntegerSerializer.INT_SIZE - recordSizeWritten;

                  recordSize = null;
                  recordSizeWritten = -1;
                  recordSizeIsWritten = true;
                  continue;
                }
              } else {
                recordSize = new byte[OIntegerSerializer.INT_SIZE];
                OIntegerSerializer.INSTANCE.serializeNative(recordContent.length, recordSize, 0);

                recordSizeWritten = buffer.remaining();
                written += recordSizeWritten;

                buffer.put(recordSize, 0, recordSizeWritten);
                continue;
              }
            }

            buffer.put(recordContent, written - OIntegerSerializer.INT_SIZE, chunkSize);
            written += chunkSize;
          }

          lastLSN = record.getLsn();
          queueSize.addAndGet(-recordContent.length);
          recordIterator.remove();
        }

        if (ptr > 0) {
          buffer.put(OCASWALPage.STOP_PAGE_OFFSET, (byte) 1);
          writeBuffer(walChannel, buffer);
          Native.free(ptr);
        }

        walChannel.force(true);
        flushedLSN = lastLSN;

        checkFreeSpace();
      } catch (IOException e)

      {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);

        throw new IllegalStateException(e);
      } catch (RuntimeException | Error e)

      {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);
        throw e;
      }
    }

    private boolean checkPresenceWritableRecords() {
      final Iterator<OWALRecord> recordIterator = records.iterator();
      assert recordIterator.hasNext();

      OWALRecord record = recordIterator.next();
      assert record instanceof OMilestoneWALRecord || record instanceof OStartWALRecord;

      while (recordIterator.hasNext()) {
        record = recordIterator.next();
        if (record instanceof OWriteableWALRecord) {
          return true;
        }
      }

      return false;
    }

    private void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
      buffer.position(OCASWALPage.MAGIC_NUMBER_OFFSET);
      buffer.putLong(OCASWALPage.MAGIC_NUMBER);

      CRC32 crc32 = new CRC32();
      buffer.position(OCASWALPage.RECORDS_OFFSET);
      crc32.update(buffer);

      buffer.position(OCASWALPage.CRC32_OFFSET);
      buffer.putInt((int) crc32.getValue());

      buffer.position(0);

      int written = 0;

      while (written < buffer.limit()) {
        written += channel.write(buffer);
      }
    }
  }
}
