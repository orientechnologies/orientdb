package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.thread.OThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque.Cursor;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque.MPSCFAAArrayDequeue;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public final class OCASDiskWriteAheadLog {
  public static final String MASTER_RECORD_EXTENSION = ".wmr";
  public static final String WAL_SEGMENT_EXTENSION   = ".wal";

  private static final int BUFFER_SIZE = 64 * 1024 * 1024;

  private static final int MASTER_RECORD_SIZE = 20;
  private static final int BATCH_READ_SIZE    = 320;
  public static final  int MAX_WRITE_BUFFERS  = 4;

  private final long walSizeHardLimit;

  private final List<OLowDiskSpaceListener>      lowDiskSpaceListeners    = new CopyOnWriteArrayList<>();
  private final List<OCheckpointRequestListener> fullCheckpointListeners  = new CopyOnWriteArrayList<>();
  private final List<OSegmentOverflowListener>   segmentOverflowListeners = new CopyOnWriteArrayList<>();

  private volatile long walSizeLimit;

  private final int maxSegmentSize;

  private final    LongAdder ongoingTXs = new LongAdder();
  private final    long      freeSpaceLimit;
  private volatile long      freeSpace  = -1;

  private final    MPSCFAAArrayDequeue<OWALRecord> records        = new MPSCFAAArrayDequeue<>();
  private volatile long                            currentSegment = 0;

  private final AtomicLong segmentSize = new AtomicLong();
  private final AtomicLong logSize     = new AtomicLong();
  private final AtomicLong queueSize   = new AtomicLong();

  private final int maxCacheSize;

  private final AtomicReference<OLogSequenceNumber> end      = new AtomicReference<>();
  private final ConcurrentSkipListSet<Long>         segments = new ConcurrentSkipListSet<>();

  private final OScheduledThreadPoolExecutorWithLogging commitExecutor;
  private final ThreadPoolExecutor                      writeExecutor;

  private final FileStore fileStore;
  private final Path      walLocation;
  private final String    storageName;

  private volatile AsynchronousFileChannel walChannel = null;

  private volatile OLogSequenceNumber flushedLSN = null;
  private volatile OLogSequenceNumber writtenLSN = null;

  private long segmentId = -1;

  private final ScheduledFuture<?> recordsWriterFuture;

  private final Path masterRecordPath;

  private volatile OLogSequenceNumber lastCheckpoint;
  private volatile boolean            useFirstMasterRecord;

  private volatile FileChannel masterRecordLSNHolder;

  private final ConcurrentNavigableMap<OLogSequenceNumber, Runnable> events = new ConcurrentSkipListMap<>();

  private final ScalableRWLock segmentLock = new ScalableRWLock();

  private final ConcurrentSkipListMap<OLogSequenceNumber, Integer> cutTillLimits = new ConcurrentSkipListMap<OLogSequenceNumber, Integer>();
  private final ScalableRWLock                                     cuttingLock   = new ScalableRWLock();

  private volatile CountDownLatch flushLatch = new CountDownLatch(0);
  private          CountDownLatch writeLatch = new CountDownLatch(0);

  private long    lastFSyncTs       = -1;
  private boolean newRecordsWritten = false;

  private final int fsyncInterval;

  public OCASDiskWriteAheadLog(String storageName, Path storagePath, final Path walPath, int maxPagesCacheSize, int maxSegmentSize,
      int commitDelay, boolean filterWALFiles, Locale locale, long walSizeHardLimit, long freeSpaceLimit, int fsyncInterval)
      throws IOException {
    commitExecutor = new OScheduledThreadPoolExecutorWithLogging(1, r -> {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task (" + storageName + ")");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    });
    commitExecutor.setMaximumPoolSize(1);

    writeExecutor = new OThreadPoolExecutorWithLogging(1, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Write Task (" + storageName + ")");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    });

    this.fsyncInterval = fsyncInterval;
    this.walSizeHardLimit = walSizeHardLimit;
    this.freeSpaceLimit = freeSpaceLimit;

    walSizeLimit = walSizeHardLimit;

    this.walLocation = calculateWalPath(storagePath, walPath);
    if (!Files.exists(walLocation)) {
      Files.createDirectories(walLocation);
    }

    masterRecordPath = walLocation.resolve(storageName + MASTER_RECORD_EXTENSION);
    masterRecordLSNHolder = FileChannel
        .open(masterRecordPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE,
            StandardOpenOption.SYNC);

    readLastCheckpointInfo();

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

    currentSegment = nextSegmentId;
    this.maxSegmentSize = maxSegmentSize;

    //we log empty record on open so end of WAL will always contain valid value
    final OStartWALRecord startRecord = new OStartWALRecord();

    startRecord.setLsn(new OLogSequenceNumber(currentSegment, OCASWALPage.RECORDS_OFFSET));
    startRecord.setDistance(0);
    startRecord.setDiskSize(OCASWALPage.RECORDS_OFFSET);

    records.offer(startRecord);

    log(new OEmptyWALRecord());

    this.recordsWriterFuture = commitExecutor
        .scheduleAtFixedRate(new RecordsWriter(), commitDelay, commitDelay, TimeUnit.MILLISECONDS);
  }

  private void readLastCheckpointInfo() throws IOException {
    boolean firstRecord = true;
    OLogSequenceNumber checkPoint = null;

    if (masterRecordLSNHolder.size() > 0) {
      OLogSequenceNumber firstMasterRecord = readMasterRecord(0);
      OLogSequenceNumber secondMasterRecord = readMasterRecord(1);

      if (firstMasterRecord == null) {
        firstRecord = true;
        checkPoint = secondMasterRecord;
      } else if (secondMasterRecord == null) {
        firstRecord = false;
        checkPoint = firstMasterRecord;
      } else {
        if (firstMasterRecord.compareTo(secondMasterRecord) >= 0) {
          checkPoint = firstMasterRecord;
          firstRecord = false;
        } else {
          checkPoint = secondMasterRecord;
          firstRecord = true;
        }
      }
    }

    this.lastCheckpoint = checkPoint;
    this.useFirstMasterRecord = firstRecord;
  }

  private void updateCheckpoint(OLogSequenceNumber checkPointLSN) throws IOException {
    if (lastCheckpoint == null || lastCheckpoint.compareTo(checkPointLSN) < 0) {
      if (useFirstMasterRecord) {
        writeMasterRecord(0, checkPointLSN);

        useFirstMasterRecord = false;
      } else {
        writeMasterRecord(1, checkPointLSN);

        useFirstMasterRecord = true;
      }

      lastCheckpoint = checkPointLSN;
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

  private OLogSequenceNumber readMasterRecord(int index) throws IOException {
    final long masterPosition = index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE);

    if (masterRecordLSNHolder.size() < masterPosition + MASTER_RECORD_SIZE) {
      OLogManager.instance().debug(this, "Cannot restore %d WAL master record for storage %s", index, storageName);
      return null;
    }

    final CRC32 crc32 = new CRC32();
    try {
      ByteBuffer buffer = ByteBuffer.allocate(MASTER_RECORD_SIZE);

      OIOUtils.readByteBuffer(buffer, masterRecordLSNHolder, masterPosition, true);
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
            .error(this, "Cannot restore %d WAL master record for storage %s crc check is failed", null, index, storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (EOFException eofException) {
      OLogManager.instance().debug(this, "Cannot restore %d WAL master record for storage %s", eofException, index, storageName);
      return null;
    }
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
      segments.add(extractSegmentId(path.getFileName().toString()));
      walSize.increment(path.toFile().length());
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

    if (endLSN == null) {
      return Collections.emptyList();
    }

    if (lsn.compareTo(endLSN) > 0) {
      return Collections.emptyList();
    }

    addCutTillLimit(lsn);
    try {
      Cursor<OWALRecord> recordCursor = records.peekFirst();
      OWALRecord record = recordCursor.getItem();
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

          recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
          if (recordCursor != null) {
            record = recordCursor.getItem();
            logRecordLSN = record.getLsn();

            if (logRecordLSN.getPosition() < 0) {
              return Collections.emptyList();
            }
          } else {
            recordCursor = records.peekFirst();
            record = recordCursor.getItem();
            logRecordLSN = record.getLsn();
            break;
          }
        }
      }

      //taken from the queue but not written to the disk yet
      ensureThatRecordWrittenOnDisk(lsn);

      return readFromDisk(lsn, limit);
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  private void ensureThatRecordWrittenOnDisk(OLogSequenceNumber lsn) {
    final OLogSequenceNumber flushedLSN = this.flushedLSN;

    while (flushedLSN == null || flushedLSN.compareTo(lsn) < 0) {
      final OLogSequenceNumber writtenLSN = this.writtenLSN;
      if (writtenLSN == null || writtenLSN.compareTo(lsn) < 0) {
        try {
          flushLatch.await();
        } catch (InterruptedException e) {
          //continue
        }
        doFlush();
      } else {
        return;
      }
    }
  }

  long segSize() {
    return segmentSize.get();
  }

  long size() {
    return logSize.get();
  }

  private List<OWriteableWALRecord> readFromDisk(OLogSequenceNumber lsn, int limit) throws IOException {
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
                OLogManager.instance()
                    .errorNoDb(this, "WAL page %d of segment %s is broken, read of records will be stopped", null, pageIndex,
                        segmentName);
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
  }

  public List<OWriteableWALRecord> next(final OLogSequenceNumber lsn, int limit) throws IOException {
    final OLogSequenceNumber endLSN = end.get();

    if (endLSN == null) {
      return Collections.emptyList();
    }

    final int compareEnd = lsn.compareTo(endLSN);

    if (compareEnd == 0) {
      return Collections.emptyList();
    }

    if (compareEnd > 0) {
      throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
    }

    addCutTillLimit(lsn);
    try {
      Cursor<OWALRecord> recordCursor = records.peekFirst();

      OWALRecord logRecord = recordCursor.getItem();
      OLogSequenceNumber logRecordLSN = logRecord.getLsn();

      while (logRecordLSN.getPosition() >= 0 && logRecordLSN.compareTo(lsn) <= 0) {
        while (true) {
          final int compare = logRecordLSN.compareTo(lsn);

          if (compare == 0) {
            recordCursor = MPSCFAAArrayDequeue.next(recordCursor);

            while (recordCursor != null) {
              OWALRecord nextRecord = recordCursor.getItem();

              if (nextRecord instanceof OWriteableWALRecord) {
                OLogSequenceNumber nextLSN = nextRecord.getLsn();

                if (nextLSN.getPosition() < 0) {
                  return Collections.emptyList();
                }

                if (nextLSN.compareTo(lsn) > 0) {
                  return Collections.singletonList((OWriteableWALRecord) nextRecord);
                } else {
                  assert nextLSN.compareTo(lsn) == 0;
                }
              }

              recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
            }

            recordCursor = records.peekFirst();
            logRecord = recordCursor.getItem();
            logRecordLSN = logRecord.getLsn();
            break;
          } else if (compare < 0) {
            recordCursor = MPSCFAAArrayDequeue.next(recordCursor);

            if (recordCursor != null) {
              logRecord = recordCursor.getItem();
              logRecordLSN = logRecord.getLsn();

              assert logRecordLSN.getPosition() >= 0;
            } else {
              recordCursor = records.peekFirst();
              logRecord = recordCursor.getItem();
              logRecordLSN = logRecord.getLsn();

              break;
            }
          } else {
            throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
          }
        }
      }

      ensureThatRecordWrittenOnDisk(lsn);

      List<OWriteableWALRecord> result;
      if (limit < 0) {
        result = readFromDisk(lsn, 0);
      } else {
        result = readFromDisk(lsn, limit + 1);
      }

      if (result.size() == 1) {
        //current record already on disk, but next record in the queue
        doFlush();
      }

      if (limit <= 0) {
        result = readFromDisk(lsn, 0);
      } else {
        result = readFromDisk(lsn, limit + 1);
      }

      return result.subList(1, result.size());
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  public void addEventAt(OLogSequenceNumber lsn, Runnable event) {
    // may be executed by multiple threads simultaneously

    final OLogSequenceNumber localFlushedLsn = flushedLSN;

    if (localFlushedLsn != null && lsn.compareTo(localFlushedLsn) <= 0)
      event.run();
    else {
      events.put(lsn, event);

      final OLogSequenceNumber potentiallyUpdatedLocalFlushedLsn = flushedLSN;
      if (potentiallyUpdatedLocalFlushedLsn != null && lsn.compareTo(potentiallyUpdatedLocalFlushedLsn) <= 0)
        commitExecutor.execute(() -> fireEventsFor(potentiallyUpdatedLocalFlushedLsn));
    }
  }

  public void delete() throws IOException {
    final List<Long> segmentsToDelete = new ArrayList<>(this.segments.size());
    segmentsToDelete.addAll(segments);

    close(false);

    Files.deleteIfExists(masterRecordPath);

    for (long segment : segmentsToDelete) {
      final String segmentName = getSegmentName(segment);
      final Path segmentPath = walLocation.resolve(segmentName);
      Files.deleteIfExists(segmentPath);
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

    cuttingLock.exclusiveLock();
    try {
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
    } finally {
      cuttingLock.exclusiveUnlock();
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

  public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) {
    OAtomicUnitStartRecord record = new OAtomicUnitStartRecord(isRollbackSupported, unitId);
    return log(record);
  }

  public OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback,
      OLogSequenceNumber startLsn, Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) {
    final OAtomicUnitEndRecord record = new OAtomicUnitEndRecord(operationUnitId, rollback, atomicOperationMetadata);
    return log(record);
  }

  public OLogSequenceNumber log(OWriteableWALRecord writeableRecord) {
    final long segSize;
    final long size;
    final OLogSequenceNumber recordLSN;

    long logSegment;
    segmentLock.sharedLock();
    try {
      if (writeableRecord instanceof OAtomicUnitStartRecord) {
        ongoingTXs.increment();
      }

      logSegment = currentSegment;
      recordLSN = doLogRecord(writeableRecord);

      final int diskSize = writeableRecord.getDiskSize();
      segSize = segmentSize.addAndGet(diskSize);
      size = logSize.addAndGet(diskSize);

      if (writeableRecord instanceof OAtomicUnitEndRecord) {
        ongoingTXs.decrement();
      }
    } finally {
      segmentLock.sharedUnlock();
    }

    long qsize = queueSize.addAndGet(writeableRecord.getBinaryContent().length);
    if (qsize >= maxCacheSize) {
      try {
        flushLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted");
      }

      qsize = queueSize.addAndGet(writeableRecord.getBinaryContent().length);

      if (qsize >= maxCacheSize) {
        doFlush();
      }
    }

    if (walSizeHardLimit < 0 && freeSpace > freeSpaceLimit) {
      walSizeLimit = size + freeSpace / 2;
    }

    if (walSizeLimit > -1 && size > walSizeLimit && segments.size() > 1) {
      for (OCheckpointRequestListener listener : fullCheckpointListeners) {
        listener.requestCheckpoint();
      }
    }

    if (segSize > maxSegmentSize) {
      for (OSegmentOverflowListener listener : segmentOverflowListeners) {
        listener.onSegmentOverflow(logSegment);
      }
    }

    return recordLSN;
  }

  public OLogSequenceNumber begin(long segmentId) {
    if (segments.contains(segmentId)) {
      return new OLogSequenceNumber(segmentId, OCASWALPage.RECORDS_OFFSET);
    }

    return null;
  }

  public boolean cutAllSegmentsSmallerThan(long segmentId) throws IOException {
    cuttingLock.exclusiveLock();
    try {
      if (lastCheckpoint != null && segmentId >= lastCheckpoint.getSegment()) {
        segmentId = lastCheckpoint.getSegment();
      }

      final OWALRecord first = records.peek();
      final OLogSequenceNumber firstLSN = first.getLsn();

      if (firstLSN.getPosition() > -1) {
        if (segmentId > firstLSN.getSegment()) {
          segmentId = firstLSN.getSegment();
        }
      }

      final Map.Entry<OLogSequenceNumber, Integer> firsEntry = cutTillLimits.firstEntry();

      if (firsEntry != null) {
        if (segmentId > firsEntry.getKey().getSegment()) {
          segmentId = firsEntry.getKey().getSegment();
        }
      }

      if (segmentId <= segments.first()) {
        return false;
      }

      boolean removed = false;

      final Iterator<Long> segmentIterator = segments.iterator();
      while (segmentIterator.hasNext()) {
        final long segment = segmentIterator.next();
        if (segment < segmentId) {
          segmentIterator.remove();

          final String segmentName = getSegmentName(segment);
          final Path segmentPath = walLocation.resolve(segmentName);
          Files.deleteIfExists(segmentPath);

          removed = true;
        } else {
          break;
        }
      }

      return removed;
    } finally {
      cuttingLock.exclusiveUnlock();
    }
  }

  public boolean cutTill(OLogSequenceNumber lsn) throws IOException {
    final long segmentId = lsn.getSegment();
    return cutAllSegmentsSmallerThan(segmentId);
  }

  public long activeSegment() {
    return currentSegment;
  }

  public void appendNewSegment() {
    segmentLock.exclusiveLock();
    try {
      if (ongoingTXs.sum() > 0) {
        throw new IllegalStateException("There are on going txs, such call can be dangerous and unpredictable");
      }

      //noinspection NonAtomicOperationOnVolatileField
      currentSegment++;

      segmentSize.set(0);
      logMilestoneRecord();
    } finally {
      segmentLock.exclusiveUnlock();
    }
  }

  public void appendSegment(long segmentIndex) {
    if (segmentIndex <= currentSegment) {
      return;
    }

    segmentLock.exclusiveLock();
    try {
      if (ongoingTXs.sum() > 0) {
        throw new IllegalStateException("There are on going txs, such call can be dangerous and unpredictable");
      }

      if (segmentIndex <= currentSegment) {
        return;
      }

      currentSegment = segmentIndex;
      segmentSize.set(0);

      logMilestoneRecord();
    } finally {
      segmentLock.exclusiveUnlock();
    }
  }

  public List<String> getWalFiles() {
    final List<String> result = new ArrayList<>();

    for (long segment : segments) {
      final String segmentName = getSegmentName(segment);
      final Path segmentPath = walLocation.resolve(segmentName);

      if (segmentPath.toFile().exists()) {
        result.add(segmentPath.toAbsolutePath().toString());
      }
    }

    return result;
  }

  public Path getWMRFile() {
    return masterRecordPath;
  }

  public void moveLsnAfter(OLogSequenceNumber lsn) {
    final long segment = lsn.getSegment() + 1;
    appendSegment(segment);
  }

  public long[] nonActiveSegments() {
    final OWALRecord firstRecord = records.peek();
    final OLogSequenceNumber firstLSN = firstRecord.getLsn();

    long maxSegment = currentSegment;

    if (firstLSN.getPosition() > -1 && firstLSN.getSegment() < maxSegment) {
      maxSegment = firstLSN.getSegment();
    }

    final List<Long> result = new ArrayList<>();
    for (long segment : segments) {
      if (segment < maxSegment) {
        result.add(segment);
      } else {
        break;
      }
    }

    final long[] segs = new long[result.size()];
    for (int i = 0; i < segs.length; i++) {
      segs[i] = result.get(i);
    }

    return segs;
  }

  public File[] nonActiveSegments(long fromSegment) {
    final OWALRecord firstRecord = records.peek();
    final OLogSequenceNumber firstLSN = firstRecord.getLsn();

    long maxSegment = currentSegment;

    if (firstLSN.getPosition() > -1 && firstLSN.getSegment() < maxSegment) {
      maxSegment = firstLSN.getSegment();
    }

    final List<File> result = new ArrayList<>();

    for (long segment : segments) {
      if (segment < maxSegment) {
        final String segmentName = getSegmentName(segment);
        final Path segmentPath = walLocation.resolve(segmentName);

        final File segFile = segmentPath.toFile();
        if (segFile.exists()) {
          result.add(segFile);
        }
      } else {
        break;
      }
    }

    return result.toArray(new File[0]);
  }

  private OLogSequenceNumber doLogRecord(OWriteableWALRecord writeableRecord) {
    writeableRecord.setBinaryContent(OWALRecordsFactory.INSTANCE.toStream(writeableRecord));
    writeableRecord.setLsn(new OLogSequenceNumber(currentSegment, -1));

    records.offer(writeableRecord);

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

    try {
      writeLatch.await();
    } catch (InterruptedException e) {
      //continue
    }

    writeExecutor.shutdown();
    try {
      if (!writeExecutor.awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
        throw new OStorageException("WAL write task for '" + storageName + "' storage cannot be stopped");

    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Cannot shutdown background WAL write thread", e);
    }
    walChannel.close();
    masterRecordLSNHolder.close();
    segments.clear();

    if (recordsWriterFuture.isDone()) {
      try {
        recordsWriterFuture.get();
      } catch (CancellationException e) {
        //ignore
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread", e);
      } catch (ExecutionException e) {
        throw new OStorageException("WAL flush task for '" + storageName + "' storage cannot be stopped");
      }
    }
  }

  private void checkFreeSpace() throws IOException {
    freeSpace = fileStore.getUsableSpace();

    //system has unlimited amount of free space
    if (freeSpace < 0)
      return;

    if (freeSpace < freeSpaceLimit) {
      for (OLowDiskSpaceListener listener : lowDiskSpaceListeners) {
        listener.lowDiskSpace(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));
      }
    }
  }

  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    lowDiskSpaceListeners.add(listener);
  }

  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    List<OLowDiskSpaceListener> itemsToRemove = new ArrayList<>();

    for (OLowDiskSpaceListener lowDiskSpaceListener : lowDiskSpaceListeners) {
      if (lowDiskSpaceListener.equals(listener)) {
        itemsToRemove.add(lowDiskSpaceListener);
      }
    }

    lowDiskSpaceListeners.removeAll(itemsToRemove);
  }

  public void addFullCheckpointListener(OCheckpointRequestListener listener) {
    fullCheckpointListeners.add(listener);
  }

  public void removeFullCheckpointListener(OCheckpointRequestListener listener) {
    List<OCheckpointRequestListener> itemsToRemove = new ArrayList<>();

    for (OCheckpointRequestListener fullCheckpointRequestListener : fullCheckpointListeners) {
      if (fullCheckpointRequestListener.equals(listener)) {
        itemsToRemove.add(fullCheckpointRequestListener);
      }
    }

    fullCheckpointListeners.removeAll(itemsToRemove);
  }

  public void addSegmentOverflowListener(OSegmentOverflowListener listener) {
    segmentOverflowListeners.add(listener);
  }

  public void removeSegmentOverflowListener(OSegmentOverflowListener listener) {
    List<OSegmentOverflowListener> itemsToRemove = new ArrayList<>();

    for (OSegmentOverflowListener segmentOverflowListener : segmentOverflowListeners) {
      if (segmentOverflowListener.equals(listener)) {
        itemsToRemove.add(segmentOverflowListener);
      }
    }

    segmentOverflowListeners.removeAll(itemsToRemove);
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
    final List<OWALRecord> unassignedList = new ArrayList<>();

    Cursor<OWALRecord> cursor = records.peekLast();
    while (cursor != null) {
      final OWALRecord record = cursor.getItem();

      final OLogSequenceNumber lsn = record.getLsn();

      if (lsn.getPosition() == -1) {
        unassignedList.add(record);
      } else {
        unassignedList.add(record);
        break;
      }

      Cursor<OWALRecord> nextCursor = MPSCFAAArrayDequeue.prev(cursor);
      if (nextCursor == null && record.getLsn().getPosition() < 0) {
        System.out.println(cursor.toString());
        throw new IllegalStateException("Invalid last record");
      }

      cursor = nextCursor;
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
    milestoneRecord.setLsn(new OLogSequenceNumber(currentSegment, -1));

    records.offer(milestoneRecord);

    calculateRecordsLSNs();

    return milestoneRecord;
  }

  public OLogSequenceNumber end() {
    return end.get();
  }

  private long calculatePosition(OWALRecord record, OWALRecord prevRecord) {
    assert
        prevRecord.getLsn().getSegment() <= record.getLsn().getSegment() :
        "prev segment " + prevRecord.getLsn().getSegment() + " segment " + record.getLsn().getSegment();

    if (prevRecord instanceof OStartWALRecord) {
      assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();

      if (record instanceof OMilestoneWALRecord) {
        record.setDistance(0);
        record.setDiskSize(prevRecord.getDiskSize());
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
        record.setDiskSize(distance + prevRecord.getDiskSize());
      }

      return prevRecord.getLsn().getPosition();
    }

    if (prevRecord instanceof OMilestoneWALRecord) {
      if (record instanceof OMilestoneWALRecord) {
        record.setDistance(0);
        //repeat previous record disk size so it will be used in first writable record
        if (prevRecord.getLsn().getSegment() == record.getLsn().getSegment()) {
          record.setDiskSize(prevRecord.getDiskSize());
          return prevRecord.getLsn().getPosition();
        }

        record.setDiskSize(prevRecord.getDiskSize());
        return OCASWALPage.RECORDS_OFFSET;
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

        assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();
        record.setDiskSize(disksize + prevRecord.getDiskSize());
      }

      return prevRecord.getLsn().getPosition();
    }

    if (record instanceof OMilestoneWALRecord) {
      if (prevRecord.getLsn().getSegment() == record.getLsn().getSegment()) {
        final long end = prevRecord.getLsn().getPosition() + prevRecord.getDistance();
        final long pageIndex = end / OCASWALPage.PAGE_SIZE;

        final long newPosition;
        final int pageOffset = (int) (end - pageIndex * OCASWALPage.PAGE_SIZE);

        if (pageOffset > OCASWALPage.RECORDS_OFFSET) {
          newPosition = (pageIndex + 1) * OCASWALPage.PAGE_SIZE + OCASWALPage.RECORDS_OFFSET;
          record.setDiskSize((int) ((pageIndex + 1) * OCASWALPage.PAGE_SIZE - end) + OCASWALPage.RECORDS_OFFSET);
        } else {
          newPosition = end;
          record.setDiskSize(OCASWALPage.RECORDS_OFFSET);
        }

        record.setDistance(0);

        return newPosition;
      } else {
        final long prevPosition = prevRecord.getLsn().getPosition();
        final long end = prevPosition + prevRecord.getDistance();
        final long pageIndex = end / OCASWALPage.PAGE_SIZE;
        final int pageOffset = (int) (end - pageIndex * OCASWALPage.PAGE_SIZE);

        if (pageOffset == OCASWALPage.RECORDS_OFFSET) {
          record.setDiskSize(OCASWALPage.RECORDS_OFFSET);
        } else {
          final int pageFreeSpace = OCASWALPage.PAGE_SIZE - pageOffset;
          record.setDiskSize(pageFreeSpace + OCASWALPage.RECORDS_OFFSET);
        }

        record.setDistance(0);

        return OCASWALPage.RECORDS_OFFSET;
      }
    }

    assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();
    final long start = prevRecord.getDistance() + prevRecord.getLsn().getPosition();
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

      if (startOffset == OCASWALPage.RECORDS_OFFSET) {
        diskSize += OCASWALPage.RECORDS_OFFSET;
      }

      record.setDiskSize(diskSize);
    }

    return start;
  }

  private void fireEventsFor(OLogSequenceNumber lsn) {
    // may be executed by only one thread at every instant of time

    final Iterator<Runnable> eventsToFire = events.headMap(lsn, true).values().iterator();
    while (eventsToFire.hasNext()) {
      eventsToFire.next().run();
      eventsToFire.remove();
    }
  }

  private String getSegmentName(long segment) {
    return storageName + "." + segment + WAL_SEGMENT_EXTENSION;
  }

  private final class RecordsWriter implements Runnable {

    @Override
    public void run() {
      try {
        if (checkPresenceWritableRecords()) {
          newRecordsWritten = true;
          flushLatch = new CountDownLatch(1);
          try {
            int bufferPage = -1;
            long rawPointer = Native.malloc(BUFFER_SIZE);
            Pointer pointer = new Pointer(rawPointer);
            ByteBuffer buffer = pointer.getByteBuffer(0, BUFFER_SIZE).order(ByteOrder.nativeOrder());

            final OMilestoneWALRecord milestoneWALRecord = logMilestoneRecord();

            OLogSequenceNumber lastLSN = null;
            OLogSequenceNumber checkPointLSN = null;

            long startPosition = -1;
            while (true) {
              OWALRecord record = records.peek();

              if (record == milestoneWALRecord) {
                break;
              }

              final OLogSequenceNumber lsn = record.getLsn();

              assert lsn.getSegment() >= segmentId;

              if (segmentId != lsn.getSegment()) {
                if (walChannel != null) {
                  if (rawPointer > 0) {

                    if ((buffer.position() & (OCASWALPage.PAGE_SIZE - 1)) != 0) {
                      buffer.put(bufferPage * OCASWALPage.PAGE_SIZE + OCASWALPage.STOP_PAGE_OFFSET, (byte) 1);
                    }

                    writeBuffer(walChannel, buffer, startPosition, rawPointer, lastLSN);
                  }

                  rawPointer = 0;
                  buffer = null;
                  pointer = null;

                  try {
                    writeLatch.await();
                  } catch (InterruptedException e) {
                    //continue
                  }
                  walChannel.force(true);
                  walChannel.close();

                  flushedLSN = writtenLSN;
                }

                segmentId = lsn.getSegment();

                final Set<OpenOption> openOptions = new HashSet<>();
                openOptions.add(StandardOpenOption.WRITE);
                openOptions.add(StandardOpenOption.CREATE_NEW);

                walChannel = AsynchronousFileChannel
                    .open(walLocation.resolve(getSegmentName(segmentId)), openOptions, writeExecutor);
                segments.add(segmentId);

                assert lsn.getPosition() == OCASWALPage.RECORDS_OFFSET;
              }

              if (record instanceof OMilestoneWALRecord || record instanceof OStartWALRecord) {
                records.poll();
              } else {
                final OWriteableWALRecord writeableRecord = (OWriteableWALRecord) record;
                if (startPosition < 0) {
                  startPosition = lsn.getPosition();
                }

                final byte[] recordContent = writeableRecord.getBinaryContent();
                assert recordContent != null;

                int written = 0;
                int bytesToWrite = OIntegerSerializer.INT_SIZE + recordContent.length;

                byte[] recordSize = null;
                int recordSizeWritten = -1;

                boolean recordSizeIsWritten = false;

                while (written < bytesToWrite) {
                  if (buffer == null || buffer.position() == buffer.limit()) {
                    if (rawPointer > 0) {
                      writeBuffer(walChannel, buffer, startPosition, rawPointer, lastLSN);
                    }

                    rawPointer = Native.malloc(BUFFER_SIZE);
                    pointer = new Pointer(rawPointer);

                    buffer = pointer.getByteBuffer(0, BUFFER_SIZE).order(ByteOrder.nativeOrder());
                    bufferPage = -1;
                  }

                  if ((buffer.position() & (OCASWALPage.PAGE_SIZE - 1)) == 0) {
                    bufferPage++;
                    pointer.setMemory(bufferPage * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE, (byte) 0);
                    buffer.position(buffer.position() + OCASWALPage.RECORDS_OFFSET);
                  }

                  final int chunkSize = Math
                      .min(bytesToWrite - written, (bufferPage + 1) * OCASWALPage.PAGE_SIZE - buffer.position());

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

                      recordSizeWritten = (bufferPage + 1) * OCASWALPage.PAGE_SIZE - buffer.position();
                      written += recordSizeWritten;

                      buffer.put(recordSize, 0, recordSizeWritten);
                      continue;
                    }
                  }

                  buffer.put(recordContent, written - OIntegerSerializer.INT_SIZE, chunkSize);
                  written += chunkSize;
                }

                lastLSN = lsn;
                if (writeableRecord.isUpdateMasterRecord()) {
                  checkPointLSN = lastLSN;
                }

                queueSize.addAndGet(-recordContent.length);
                records.poll();
              }
            }

            if (rawPointer > 0) {
              if ((buffer.position() & (OCASWALPage.PAGE_SIZE - 1)) != 0) {
                buffer.put(bufferPage * OCASWALPage.PAGE_SIZE + OCASWALPage.STOP_PAGE_OFFSET, (byte) 1);
              }

              writeBuffer(walChannel, buffer, startPosition, rawPointer, lastLSN);
            }

            if (checkPointLSN != null) {
              updateCheckpoint(checkPointLSN);
            }
          } finally {
            flushLatch.countDown();
          }
        }

        final long ts = System.nanoTime();

        if (ts - lastFSyncTs > fsyncInterval * 1_000_000) {
          try {
            if (newRecordsWritten) {
              writeExecutor.submit(() -> {
                try {
                  walChannel.force(true);
                  flushedLSN = writtenLSN;
                } catch (IOException e) {
                  OLogManager.instance().error(this, "Error during FSync of WAL data", e);
                }
              });
              newRecordsWritten = false;
            }

            checkFreeSpace();
          } finally {
            lastFSyncTs = ts;
          }
        }
      } catch (IOException e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);
        throw new IllegalStateException(e);
      } catch (RuntimeException | Error e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);
        throw e;
      }
    }

    private boolean checkPresenceWritableRecords() {
      Cursor<OWALRecord> recordCursor = records.peekFirst();

      OWALRecord record = recordCursor.getItem();
      assert record instanceof OMilestoneWALRecord || record instanceof OStartWALRecord;

      recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
      while (recordCursor != null) {
        record = recordCursor.getItem();
        if (record instanceof OWriteableWALRecord) {
          return true;
        }

        recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
      }

      return false;
    }

    private void writeBuffer(AsynchronousFileChannel channel, ByteBuffer buffer, long position, long pointer,
        OLogSequenceNumber lastLSN) {
      if (position < 0) {
        return;
      }

      final long startPage = position / OCASWALPage.PAGE_SIZE;
      final long writePosition = startPage * OCASWALPage.PAGE_SIZE;
      final int maxPage = (buffer.position() + OCASWALPage.PAGE_SIZE - 1) / OCASWALPage.PAGE_SIZE;

      for (int start = 0; start < maxPage * OCASWALPage.PAGE_SIZE; start += OCASWALPage.PAGE_SIZE) {
        buffer.limit(start + OCASWALPage.PAGE_SIZE);

        buffer.position(start + OCASWALPage.MAGIC_NUMBER_OFFSET);
        buffer.putLong(OCASWALPage.MAGIC_NUMBER);

        CRC32 crc32 = new CRC32();
        buffer.position(start + OCASWALPage.RECORDS_OFFSET);
        crc32.update(buffer);

        buffer.position(start + OCASWALPage.CRC32_OFFSET);
        buffer.putInt((int) crc32.getValue());
      }

      buffer.position(0);
      buffer.limit(maxPage * OCASWALPage.PAGE_SIZE);

      try {
        writeLatch.await();
      } catch (InterruptedException e) {
        //continue
      }

      writeLatch = new CountDownLatch(1);
      writeExecutor.submit(() -> {
        channel.write(buffer, writePosition, null, new CompletionHandler<Integer, Object>() {
          @Override
          public void completed(Integer result, Object attachment) {
            if (result == buffer.limit()) {
              Native.free(pointer);
              if (lastLSN != null) {
                writtenLSN = lastLSN;
              }

              writeLatch.countDown();
            } else {
              channel.write(buffer, writePosition + buffer.position(), null, this);
            }
          }

          @Override
          public void failed(Throwable exc, Object attachment) {
            OLogManager.instance().error(this, "Error during WAL write", exc);
            Native.free(pointer);
            writeLatch.countDown();
          }
        });
      });
    }
  }
}
