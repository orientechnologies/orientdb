package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.jna.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.thread.OScheduledThreadPoolExecutorWithLogging;
import com.orientechnologies.common.thread.OThreadPoolExecutorWithLogging;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque.Cursor;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque.MPSCFAAArrayDequeue;
import com.sun.jna.Platform;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public final class OCASDiskWriteAheadLog implements OWriteAheadLog {
  private final static XXHashFactory xxHashFactory = XXHashFactory.fastestInstance();
  private static final int           XX_SEED       = 0x9747b28c;

  private static final int MASTER_RECORD_SIZE = 20;
  private static final int BATCH_READ_SIZE    = 320;

  protected static final int DEFAULT_MAX_CACHE_SIZE = Integer.MAX_VALUE;

  private static final OScheduledThreadPoolExecutorWithLogging commitExecutor;
  private static final OThreadPoolExecutorWithLogging          writeExecutor;

  static {
    commitExecutor = new OScheduledThreadPoolExecutorWithLogging(1, r -> {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    });

    writeExecutor = new OThreadPoolExecutorWithLogging(1, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Write Task Thread)");
      thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      return thread;
    });

    commitExecutor.setMaximumPoolSize(1);
  }

  private final boolean keepSingleWALSegment;

  private final long walSizeHardLimit;

  private final List<OLowDiskSpaceListener>      lowDiskSpaceListeners    = new CopyOnWriteArrayList<>();
  private final List<OCheckpointRequestListener> fullCheckpointListeners  = new CopyOnWriteArrayList<>();
  private final List<OSegmentOverflowListener>   segmentOverflowListeners = new CopyOnWriteArrayList<>();

  private volatile long walSizeLimit;

  private final long segmentsInterval;

  private final long maxSegmentSize;

  private final long freeSpaceLimit;

  private final MPSCFAAArrayDequeue<OWALRecord> records = new MPSCFAAArrayDequeue<>();

  private volatile long currentSegment;

  private final AtomicLong segmentSize = new AtomicLong();
  private final AtomicLong logSize     = new AtomicLong();
  private final AtomicLong queueSize   = new AtomicLong();

  private final int maxCacheSize;

  private final AtomicReference<OLogSequenceNumber> end      = new AtomicReference<>();
  private final ConcurrentSkipListSet<Long>         segments = new ConcurrentSkipListSet<>();

  private final FileStore fileStore;
  private final Path      walLocation;
  private final String    storageName;

  private final ODirectMemoryAllocator allocator = ODirectMemoryAllocator.instance();

  private final int     blockSize;
  private final boolean allowDirectIO;
  private final int     pageSize;
  private final int     maxRecordSize;

  private volatile OWALFile walFile = null;

  private volatile OLogSequenceNumber flushedLSN = null;

  private final AtomicReference<WrittenUpTo> writtenUpTo = new AtomicReference<>();
  private       long                         segmentId   = -1;

  private volatile ScheduledFuture<?> recordsWriterFuture;

  private final Path masterRecordPath;

  private volatile OLogSequenceNumber lastCheckpoint;

  private volatile boolean useFirstMasterRecord;

  private final FileChannel masterRecordLSNHolder;

  private final ConcurrentNavigableMap<OLogSequenceNumber, Runnable> events = new ConcurrentSkipListMap<>();

  private final ScalableRWLock segmentLock = new ScalableRWLock();

  private final TreeMap<OLogSequenceNumber, Integer> cutTillLimits = new TreeMap<>();
  private final ScalableRWLock                       cuttingLock   = new ScalableRWLock();

  private final ConcurrentLinkedQueue<OPair<Long, OWALFile>> fileCloseQueue     = new ConcurrentLinkedQueue<>();
  private final AtomicInteger                                fileCloseQueueSize = new AtomicInteger();

  private final    AtomicReference<CountDownLatch> flushLatch        = new AtomicReference<>(new CountDownLatch(0));
  private volatile Future<?>                       writeFuture       = null;
  //not volatile because used only inside of write thread.
  private          OLogSequenceNumber              writtenCheckpoint = null;

  private          long lastFSyncTs = -1;
  private final    int  fsyncInterval;
  private volatile long segmentAdditionTs;

  private final int commitDelay;

  private long currentPosition = 0;

  private boolean useFirstBuffer = true;

  private ByteBuffer writeBuffer          = null;
  private OPointer   writeBufferPointer   = null;
  private int        writeBufferPageIndex = -1;

  private final ByteBuffer writeBufferOne;
  private final OPointer   writeBufferPointerOne;

  private final ByteBuffer writeBufferTwo;
  private final OPointer   writeBufferPointerTwo;

  private OLogSequenceNumber lastLSN       = null;
  private OLogSequenceNumber checkPointLSN = null;

  private final boolean callFsync;

  private final boolean printPerformanceStatistic;
  private final int     statisticPrintInterval;

  private volatile long bytesWrittenSum  = 0;
  private volatile long bytesWrittenTime = 0;

  private volatile long fsyncTime  = 0;
  private volatile long fsyncCount = 0;

  private final LongAdder threadsWaitingSum   = new LongAdder();
  private final LongAdder threadsWaitingCount = new LongAdder();

  private long reportTs = -1;

  private volatile boolean stopWrite = false;

  public OCASDiskWriteAheadLog(final String storageName, final Path storagePath, final Path walPath, final int maxPagesCacheSize,
      final int bufferSize, long segmentsInterval, final long maxSegmentSize, final int commitDelay, final boolean filterWALFiles,
      final Locale locale, final long walSizeHardLimit, final long freeSpaceLimit, final int fsyncInterval, boolean allowDirectIO,
      boolean keepSingleWALSegment, boolean callFsync, boolean printPerformanceStatistic, int statisticPrintInterval)
      throws IOException {

    int bufferSize1 = bufferSize * 1024 * 1024;

    this.segmentsInterval = segmentsInterval;
    this.keepSingleWALSegment = keepSingleWALSegment;
    this.callFsync = callFsync;
    this.printPerformanceStatistic = printPerformanceStatistic;
    this.statisticPrintInterval = statisticPrintInterval;

    this.fsyncInterval = fsyncInterval;
    this.walSizeHardLimit = walSizeHardLimit;
    this.freeSpaceLimit = freeSpaceLimit;

    walSizeLimit = walSizeHardLimit;

    this.walLocation = calculateWalPath(storagePath, walPath);

    if (!Files.exists(walLocation)) {
      Files.createDirectories(walLocation);
    }

    this.fileStore = Files.getFileStore(walLocation);
    this.storageName = storageName;

    if (allowDirectIO) {
      blockSize = calculateBlockSize(walLocation.toAbsolutePath().toString());
    } else {
      blockSize = -1;
    }

    if (blockSize > 0) {
      this.allowDirectIO = true;
      OLogManager.instance()
          .infoNoDb(this, "Direct IO for WAL located in %s is allowed with block size %d bytes.", walLocation.toString(),
              blockSize);
    } else {
      this.allowDirectIO = false;
    }

    if (this.allowDirectIO) {
      pageSize = blockSize;
      maxRecordSize = pageSize - OCASWALPage.RECORDS_OFFSET;
    } else {
      pageSize = OCASWALPage.DEFAULT_PAGE_SIZE;
      maxRecordSize = OCASWALPage.DEFAULT_MAX_RECORD_SIZE;
    }

    OLogManager.instance().infoNoDb(this, "Page size for WAL located in %s is set to %d bytes.", walLocation.toString(), pageSize);

    this.maxCacheSize = multiplyIntsWithOverflowDefault(maxPagesCacheSize, pageSize, DEFAULT_MAX_CACHE_SIZE);

    masterRecordPath = walLocation.resolve(storageName + MASTER_RECORD_EXTENSION);
    masterRecordLSNHolder = FileChannel
        .open(masterRecordPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE,
            StandardOpenOption.SYNC);

    readLastCheckpointInfo();

    logSize.set(initSegmentSet(filterWALFiles, locale));

    final long nextSegmentId;

    if (segments.isEmpty()) {
      nextSegmentId = 1;
    } else {
      nextSegmentId = segments.last() + 1;
    }

    currentSegment = nextSegmentId;
    this.maxSegmentSize = maxSegmentSize;
    this.segmentAdditionTs = System.nanoTime();

    //we log empty record on open so end of WAL will always contain valid value
    final OStartWALRecord startRecord = new OStartWALRecord();

    startRecord.setLsn(new OLogSequenceNumber(currentSegment, OCASWALPage.RECORDS_OFFSET));
    startRecord.setDistance(0);
    startRecord.setDiskSize(OCASWALPage.RECORDS_OFFSET);

    records.offer(startRecord);

    writtenUpTo.set(new WrittenUpTo(new OLogSequenceNumber(currentSegment, 0), 0));

    this.commitDelay = commitDelay;

    writeBufferPointerOne = allocator.allocate(bufferSize1, blockSize);
    writeBufferOne = writeBufferPointerOne.getNativeByteBuffer().order(ByteOrder.nativeOrder());

    writeBufferPointerTwo = allocator.allocate(bufferSize1, blockSize);
    writeBufferTwo = writeBufferPointerTwo.getNativeByteBuffer().order(ByteOrder.nativeOrder());

    log(new OEmptyWALRecord());

    this.recordsWriterFuture = commitExecutor.schedule(new RecordsWriter(false, false, true), commitDelay, TimeUnit.MILLISECONDS);
    flush();
  }

  private int multiplyIntsWithOverflowDefault(final int maxPagesCacheSize, final int pageSize, final int defaultValue) {
    long maxCacheSize = (long)maxPagesCacheSize * (long)pageSize;
    if ((int)maxCacheSize != maxCacheSize) {
      return defaultValue;
    }
    return (int)maxCacheSize;
  }

  public int pageSize() {
    return pageSize;
  }

  protected int maxCacheSize() {
    return maxCacheSize;
  }

  private static int calculateBlockSize(String path) {
    if (!Platform.isLinux()) {
      return -1;
    }

    final int linuxVersion = 0;
    final int majorRev = 1;
    final int minorRev = 2;

    List<Integer> versionNumbers = new ArrayList<>();
    for (String v : System.getProperty("os.version").split("[.\\-]")) {
      if (v.matches("\\d")) {
        versionNumbers.add(Integer.parseInt(v));
      }
    }

    if (versionNumbers.get(linuxVersion) < 2) {
      return -1;
    } else if (versionNumbers.get(linuxVersion) == 2) {
      if (versionNumbers.get(majorRev) < 4) {
        return -1;
      } else if (versionNumbers.get(majorRev) == 4 && versionNumbers.get(minorRev) < 10) {
        return -1;
      }
    }

    final int _PC_REC_XFER_ALIGN = 0x11;

    int fsBlockSize = ONative.instance().pathconf(path, _PC_REC_XFER_ALIGN);
    int pageSize = ONative.instance().getpagesize();
    fsBlockSize = lcm(fsBlockSize, pageSize);

    // just being completely paranoid:
    // (512 is the rule for 2.6+ kernels)
    fsBlockSize = lcm(fsBlockSize, 512);

    if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
      return -1;
    }

    return fsBlockSize;
  }

  private static int lcm(long x, long y) {
    long g = x; // will hold gcd
    long yc = y;

    // get the gcd first
    while (yc != 0) {
      long t = g;
      g = yc;
      yc = t % yc;
    }

    return (int) (x * y / g);
  }

  private void readLastCheckpointInfo() throws IOException {
    boolean firstRecord = true;
    OLogSequenceNumber checkPoint = null;

    if (masterRecordLSNHolder.size() > 0) {
      final OLogSequenceNumber firstMasterRecord = readMasterRecord(0);
      final OLogSequenceNumber secondMasterRecord = readMasterRecord(1);

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

  private void updateCheckpoint(final OLogSequenceNumber checkPointLSN) throws IOException {
    if (checkPointLSN == null) {
      return;
    }

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

  private void writeMasterRecord(final int index, final OLogSequenceNumber masterRecord) throws IOException {
    masterRecordLSNHolder.position();
    final CRC32 crc32 = new CRC32();

    final byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getSegment(), serializedLSN, 0);
    OLongSerializer.INSTANCE.serializeLiteral(masterRecord.getPosition(), serializedLSN, OLongSerializer.LONG_SIZE);
    crc32.update(serializedLSN, 0, serializedLSN.length);

    final ByteBuffer buffer = ByteBuffer.allocate(MASTER_RECORD_SIZE);

    buffer.putInt((int) crc32.getValue());
    buffer.putLong(masterRecord.getSegment());
    buffer.putLong(masterRecord.getPosition());
    buffer.rewind();

    OIOUtils.writeByteBuffer(buffer, masterRecordLSNHolder, index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE));
  }

  private OLogSequenceNumber readMasterRecord(final int index) throws IOException {
    final long masterPosition = index * (OIntegerSerializer.INT_SIZE + 2L * OLongSerializer.LONG_SIZE);

    if (masterRecordLSNHolder.size() < masterPosition + MASTER_RECORD_SIZE) {
      OLogManager.instance().debugNoDb(this, "Cannot restore %d WAL master record for storage %s", null, index, storageName);
      return null;
    }

    final CRC32 crc32 = new CRC32();
    try {
      final ByteBuffer buffer = ByteBuffer.allocate(MASTER_RECORD_SIZE);

      OIOUtils.readByteBuffer(buffer, masterRecordLSNHolder, masterPosition, true);
      buffer.rewind();

      final int firstCRC = buffer.getInt();
      final long segment = buffer.getLong();
      final long position = buffer.getLong();

      final byte[] serializedLSN = new byte[2 * OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serializeLiteral(segment, serializedLSN, 0);
      OLongSerializer.INSTANCE.serializeLiteral(position, serializedLSN, OLongSerializer.LONG_SIZE);
      crc32.update(serializedLSN, 0, serializedLSN.length);

      if (firstCRC != ((int) crc32.getValue())) {
        OLogManager.instance()
            .errorNoDb(this, "Cannot restore %d WAL master record for storage %s crc check is failed", null, index, storageName);
        return null;
      }

      return new OLogSequenceNumber(segment, position);
    } catch (final EOFException eofException) {
      OLogManager.instance()
          .debugNoDb(this, "Cannot restore %d WAL master record for storage %s", eofException, index, storageName);
      return null;
    }
  }

  private long initSegmentSet(final boolean filterWALFiles, final Locale locale) throws IOException {
    final Stream<Path> walFiles;

    final OModifiableLong walSize = new OModifiableLong();
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

  private static long extractSegmentId(final String name) {
    final Matcher matcher = Pattern.compile("^.*\\.(\\d+)\\.wal$").matcher(name);

    final boolean matches = matcher.find();
    assert matches;

    final String order = matcher.group(1);
    try {
      return Long.parseLong(order);
    } catch (final NumberFormatException e) {
      // never happen
      throw new IllegalStateException(e);
    }
  }

  private static boolean validateName(String name, String storageName, final Locale locale) {
    name = name.toLowerCase(locale);
    storageName = storageName.toLowerCase(locale);

    if (!name.endsWith(".wal"))
      return false;

    final int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final String walStorageName = name.substring(0, walOrderStartIndex);
    if (!storageName.equals(walStorageName))
      return false;

    final int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    final String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      Integer.parseInt(walOrder);
    } catch (final NumberFormatException ignore) {
      return false;
    }

    return true;
  }

  private static boolean validateSimpleName(String name, final Locale locale) {
    name = name.toLowerCase(locale);

    if (!name.endsWith(".wal"))
      return false;

    final int walOrderStartIndex = name.indexOf('.');
    if (walOrderStartIndex == name.length() - 4)
      return false;

    final int walOrderEndIndex = name.indexOf('.', walOrderStartIndex + 1);

    final String walOrder = name.substring(walOrderStartIndex + 1, walOrderEndIndex);
    try {
      Integer.parseInt(walOrder);
    } catch (final NumberFormatException ignore) {
      return false;
    }

    return true;
  }

  private static Path calculateWalPath(final Path storagePath, final Path walPath) {
    if (walPath == null)
      return storagePath;

    return walPath;
  }

  public List<OWriteableWALRecord> read(final OLogSequenceNumber lsn, final int limit) throws IOException {
    addCutTillLimit(lsn);
    try {
      final OLogSequenceNumber begin = begin();
      final OLogSequenceNumber endLSN = end.get();

      if (begin.compareTo(lsn) > 0) {
        return Collections.emptyList();
      }

      if (lsn.compareTo(endLSN) > 0) {
        return Collections.emptyList();
      }

      Cursor<OWALRecord> recordCursor = records.peekFirst();
      assert recordCursor != null;
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
            assert recordCursor != null;
            record = recordCursor.getItem();
            logRecordLSN = record.getLsn();
            break;
          }
        }
      }

      //ensure that next record is written on disk
      OLogSequenceNumber writtenLSN = this.writtenUpTo.get().lsn;
      while (writtenLSN == null || writtenLSN.compareTo(lsn) < 0) {
        try {
          flushLatch.get().await();
        } catch (final InterruptedException e) {
          OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
        }

        writtenLSN = this.writtenUpTo.get().lsn;
        assert writtenLSN != null;

        if (writtenLSN.compareTo(lsn) < 0) {
          doFlush(false);
          waitTillWriteWillBeFinished();
        }

        writtenLSN = this.writtenUpTo.get().lsn;
      }

      return readFromDisk(lsn, limit);
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  private void waitTillWriteWillBeFinished() {
    final Future<?> wf = writeFuture;
    if (wf != null) {
      try {
        wf.get();
      } catch (final InterruptedException e) {
        throw OException.wrapException(new OStorageException("WAL write for storage " + storageName + " was interrupted"), e);
      } catch (final ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during WAL write for storage " + storageName), e);
      }
    }
  }

  OLogSequenceNumber lastCheckpoint() {
    return lastCheckpoint;
  }

  long segSize() {
    return segmentSize.get();
  }

  long size() {
    return logSize.get();
  }

  private List<OWriteableWALRecord> readFromDisk(final OLogSequenceNumber lsn, final int limit) throws IOException {
    final List<OWriteableWALRecord> result = new ArrayList<>();
    long position = lsn.getPosition();
    long pageIndex = position / pageSize;
    long segment = lsn.getSegment();

    int pagesRead = 0;

    final NavigableSet<Long> segs = segments.tailSet(segment);
    if (segs.isEmpty() || segs.first() > segment) {
      return Collections.emptyList();
    }
    final Iterator<Long> segmentsIterator = segs.iterator();

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

        if (Files.exists(segmentPath)) {
          try (final OWALFile file = OWALFile.createReadWALFile(segmentPath, allowDirectIO, blockSize)) {
            long chSize = Files.size(segmentPath);
            final WrittenUpTo written = this.writtenUpTo.get();

            if (segment == written.lsn.getSegment()) {
              chSize = Math.min(chSize, written.position);
            }

            while (pageIndex * pageSize < chSize) {
              file.position(pageIndex * pageSize);

              final OPointer ptr = allocator.allocate(pageSize, blockSize);
              try {
                final ByteBuffer buffer = ptr.getNativeByteBuffer().order(ByteOrder.nativeOrder());
                file.readBuffer(buffer);
                pagesRead++;

                if (pageIsBroken(buffer, pageSize)) {
                  OLogManager.instance()
                      .errorNoDb(this, "WAL page %d of segment %s is broken, read of records will be stopped", null, pageIndex,
                          segmentName);
                  return result;
                }

                buffer.position((int) (position - pageIndex * pageSize));
                while (buffer.remaining() > 0) {
                  if (recordLen == -1) {
                    if (recordLenBytes == null) {
                      lsnPos = pageIndex * pageSize + buffer.position();

                      if (buffer.remaining() >= OIntegerSerializer.INT_SIZE) {
                        recordLen = buffer.getInt();
                      } else {
                        recordLenBytes = new byte[OIntegerSerializer.INT_SIZE];
                        recordLenRead = buffer.remaining();

                        buffer.get(recordLenBytes, 0, recordLenRead);
                        continue;
                      }
                    } else {
                      buffer.get(recordLenBytes, recordLenRead, OIntegerSerializer.INT_SIZE - recordLenRead);
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
                allocator.deallocate(ptr);
              }

              pageIndex++;
              position = pageIndex * pageSize + OCASWALPage.RECORDS_OFFSET;
            }

            //we can jump to a new segment and skip and of the current file because of thread racing
            //so we stop here to start to read from next batch
            if (segment == written.lsn.getSegment()) {
              break;
            }
          }
        } else {
          break;
        }

        pageIndex = 0;
        position = OCASWALPage.RECORDS_OFFSET;
      } else {
        break;
      }
    }

    return result;
  }

  public List<OWriteableWALRecord> next(final OLogSequenceNumber lsn, final int limit) throws IOException {
    addCutTillLimit(lsn);
    try {
      final OLogSequenceNumber begin = begin();

      if (begin.compareTo(lsn) > 0) {
        return Collections.emptyList();
      }

      final OLogSequenceNumber end = this.end.get();
      if (lsn.compareTo(end) >= 0) {
        return Collections.emptyList();
      }

      Cursor<OWALRecord> recordCursor = records.peekFirst();

      assert recordCursor != null;
      OWALRecord logRecord = recordCursor.getItem();
      OLogSequenceNumber logRecordLSN = logRecord.getLsn();

      while (logRecordLSN.getPosition() >= 0 && logRecordLSN.compareTo(lsn) <= 0) {
        while (true) {
          final int compare = logRecordLSN.compareTo(lsn);

          if (compare == 0) {
            recordCursor = MPSCFAAArrayDequeue.next(recordCursor);

            while (recordCursor != null) {
              final OWALRecord nextRecord = recordCursor.getItem();

              if (nextRecord instanceof OWriteableWALRecord) {
                final OLogSequenceNumber nextLSN = nextRecord.getLsn();

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
            assert recordCursor != null;
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
              assert recordCursor != null;
              logRecord = recordCursor.getItem();
              logRecordLSN = logRecord.getLsn();

              break;
            }
          } else {
            throw new IllegalArgumentException("Invalid LSN was passed " + lsn);
          }
        }
      }

      //ensure that next record is written on disk
      OLogSequenceNumber writtenLSN = this.writtenUpTo.get().lsn;
      while (writtenLSN == null || writtenLSN.compareTo(lsn) <= 0) {
        try {
          flushLatch.get().await();
        } catch (final InterruptedException e) {
          OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
        }

        writtenLSN = this.writtenUpTo.get().lsn;
        assert writtenLSN != null;

        if (writtenLSN.compareTo(lsn) <= 0) {
          doFlush(false);

          waitTillWriteWillBeFinished();
        }
        writtenLSN = this.writtenUpTo.get().lsn;
      }

      final List<OWriteableWALRecord> result;
      if (limit <= 0) {
        result = readFromDisk(lsn, 0);
      } else {
        result = readFromDisk(lsn, limit + 1);
      }
      if (result.isEmpty()) {
        return result;
      }
      return result.subList(1, result.size());
    } finally {
      removeCutTillLimit(lsn);
    }
  }

  public void addEventAt(final OLogSequenceNumber lsn, final Runnable event) {
    // may be executed by multiple threads simultaneously

    final OLogSequenceNumber localFlushedLsn = flushedLSN;

    if (localFlushedLsn != null && lsn.compareTo(localFlushedLsn) <= 0)
      event.run();
    else {
      events.put(lsn, event);

      final OLogSequenceNumber potentiallyUpdatedLocalFlushedLsn = flushedLSN;
      if (potentiallyUpdatedLocalFlushedLsn != null && lsn.compareTo(potentiallyUpdatedLocalFlushedLsn) <= 0) {
        commitExecutor.execute(() -> fireEventsFor(potentiallyUpdatedLocalFlushedLsn));
      }
    }
  }

  public void delete() throws IOException {
    final List<Long> segmentsToDelete = new ArrayList<>(this.segments.size());
    segmentsToDelete.addAll(segments);

    close(false);

    Files.deleteIfExists(masterRecordPath);

    for (final long segment : segmentsToDelete) {
      final String segmentName = getSegmentName(segment);
      final Path segmentPath = walLocation.resolve(segmentName);
      Files.deleteIfExists(segmentPath);
    }
  }

  private static boolean pageIsBroken(final ByteBuffer buffer, int walPageSize) {
    buffer.position(OCASWALPage.MAGIC_NUMBER_OFFSET);

    if (buffer.getLong() != OCASWALPage.MAGIC_NUMBER) {
      return true;
    }

    final int pageSize = buffer.getShort(OCASWALPage.PAGE_SIZE_OFFSET);
    if (pageSize == 0 || pageSize > walPageSize) {
      return true;
    }

    buffer.limit(pageSize);

    buffer.position(OCASWALPage.RECORDS_OFFSET);
    final XXHash64 hash64 = xxHashFactory.hash64();

    final long hash = hash64.hash(buffer, XX_SEED);

    buffer.position(OCASWALPage.XX_OFFSET);
    return hash != buffer.getLong();
  }

  public void addCutTillLimit(final OLogSequenceNumber lsn) {
    if (lsn == null)
      throw new NullPointerException();

    cuttingLock.sharedLock();
    try {
      cutTillLimits.merge(lsn, 1, Integer::sum);
    } finally {
      cuttingLock.sharedUnlock();
    }
  }

  public void removeCutTillLimit(final OLogSequenceNumber lsn) {
    if (lsn == null)
      throw new NullPointerException();

    cuttingLock.sharedLock();
    try {
      cutTillLimits.compute(lsn, (key, oldCounter) -> {
        if (oldCounter == null) {
          throw new IllegalArgumentException(String.format("Limit %s is going to be removed but it was not added", lsn));
        }

        final int newCounter = oldCounter - 1;
        if (newCounter == 0) {
          return null;
        }

        return newCounter;
      });
    } finally {
      cuttingLock.sharedUnlock();
    }
  }

  public OLogSequenceNumber logAtomicOperationStartRecord(final boolean isRollbackSupported, final long unitId) {
    final OAtomicUnitStartRecordV2 record = new OAtomicUnitStartRecordV2(isRollbackSupported, unitId);
    return log(record);
  }

  public OLogSequenceNumber logAtomicOperationStartRecord(final boolean isRollbackSupported, final long unitId, byte[] metadata) {
    final OAtomicUnitStartMetadataRecord record = new OAtomicUnitStartMetadataRecord(isRollbackSupported, unitId, metadata);
    return log(record);
  }

  public OLogSequenceNumber logAtomicOperationEndRecord(final long operationUnitId, final boolean rollback,
      final OLogSequenceNumber startLsn, final Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) {
    final OAtomicUnitEndRecordV2 record = new OAtomicUnitEndRecordV2(operationUnitId, rollback, atomicOperationMetadata);
    return log(record);
  }

  @Override
  public OLogSequenceNumber logFuzzyCheckPointStart(final OLogSequenceNumber flushedLsn) {
    final OFuzzyCheckpointStartRecord record = new OFuzzyCheckpointStartRecord(lastCheckpoint, flushedLsn);
    log(record);
    return record.getLsn();
  }

  @Override
  public OLogSequenceNumber logFuzzyCheckPointEnd() {
    final OFuzzyCheckpointEndRecord record = new OFuzzyCheckpointEndRecord();
    log(record);
    return record.getLsn();
  }

  @Override
  public OLogSequenceNumber logFullCheckpointStart() {
    return log(new OFullCheckpointStartRecord(lastCheckpoint));
  }

  @Override
  public OLogSequenceNumber logFullCheckpointEnd() {
    return log(new OCheckpointEndRecord());
  }

  @Override
  public OLogSequenceNumber getLastCheckpoint() {
    return lastCheckpoint;
  }

  public OLogSequenceNumber log(final OWriteableWALRecord writeableRecord) {
    final long segSize;
    final long size;
    final OLogSequenceNumber recordLSN;

    long logSegment;
    segmentLock.sharedLock();
    try {
      logSegment = currentSegment;
      recordLSN = doLogRecord(writeableRecord);

      final int diskSize = writeableRecord.getDiskSize();
      segSize = segmentSize.addAndGet(diskSize);
      size = logSize.addAndGet(diskSize);

      if (segSize == diskSize) {
        segments.add(currentSegment);
      }
    } finally {
      segmentLock.sharedUnlock();
    }

    long qsize = queueSize.addAndGet(writeableRecord.getDiskSize());
    if (qsize >= maxCacheSize) {
      threadsWaitingCount.increment();
      try {
        long startTs = 0;
        if (printPerformanceStatistic) {
          startTs = System.nanoTime();
        }
        flushLatch.get().await();
        if (printPerformanceStatistic) {
          final long endTs = System.nanoTime();
          threadsWaitingSum.add(endTs - startTs);
        }
      } catch (final InterruptedException e) {
        OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
      }

      qsize = queueSize.get();

      if (qsize >= maxCacheSize) {
        long startTs = 0;
        if (printPerformanceStatistic) {
          startTs = System.nanoTime();
        }
        doFlush(false);
        if (printPerformanceStatistic) {
          final long endTs = System.nanoTime();
          threadsWaitingSum.add(endTs - startTs);
        }
      }
    }

    if (keepSingleWALSegment && segments.size() > 1) {
      for (final OCheckpointRequestListener listener : fullCheckpointListeners) {
        listener.requestCheckpoint();
      }
    } else if (walSizeLimit > -1 && size > walSizeLimit && segments.size() > 1) {
      for (final OCheckpointRequestListener listener : fullCheckpointListeners) {
        listener.requestCheckpoint();
      }
    }

    if (segSize > maxSegmentSize) {
      for (final OSegmentOverflowListener listener : segmentOverflowListeners) {
        listener.onSegmentOverflow(logSegment);
      }
    }

    return recordLSN;
  }

  public OLogSequenceNumber begin() {
    final long first = segments.first();
    return new OLogSequenceNumber(first, OCASWALPage.RECORDS_OFFSET);
  }

  public OLogSequenceNumber begin(final long segmentId) {
    if (segments.contains(segmentId)) {
      return new OLogSequenceNumber(segmentId, OCASWALPage.RECORDS_OFFSET);
    }

    return null;
  }

  public boolean cutAllSegmentsSmallerThan(long segmentId) throws IOException {
    cuttingLock.exclusiveLock();
    try {
      segmentLock.sharedLock();
      try {
        if (segmentId > currentSegment) {
          segmentId = currentSegment;
        }

        final Map.Entry<OLogSequenceNumber, Integer> firsEntry = cutTillLimits.firstEntry();

        if (firsEntry != null) {
          if (segmentId > firsEntry.getKey().getSegment()) {
            segmentId = firsEntry.getKey().getSegment();
          }
        }

        final OLogSequenceNumber written = writtenUpTo.get().lsn;
        if (segmentId > written.getSegment()) {
          segmentId = written.getSegment();
        }

        if (segmentId <= segments.first()) {
          return false;
        }

        OPair<Long, OWALFile> pair = fileCloseQueue.poll();
        while (pair != null) {
          final OWALFile file = pair.value;

          fileCloseQueueSize.decrementAndGet();
          if (pair.key >= segmentId) {
            if (callFsync) {
              file.force(true);
            }

            file.close();
            break;
          } else {
            file.close();
          }
          pair = fileCloseQueue.poll();
        }

        boolean removed = false;

        final Iterator<Long> segmentIterator = segments.iterator();
        while (segmentIterator.hasNext()) {
          final long segment = segmentIterator.next();
          if (segment < segmentId) {
            segmentIterator.remove();

            final String segmentName = getSegmentName(segment);
            final Path segmentPath = walLocation.resolve(segmentName);
            if (Files.exists(segmentPath)) {
              final long length = Files.size(segmentPath);
              Files.delete(segmentPath);
              logSize.addAndGet(-length);
              removed = true;
            }
          } else {
            break;
          }
        }

        return removed;
      } finally {
        segmentLock.sharedUnlock();
      }
    } finally {
      cuttingLock.exclusiveUnlock();
    }
  }

  public boolean cutTill(final OLogSequenceNumber lsn) throws IOException {
    final long segmentId = lsn.getSegment();
    return cutAllSegmentsSmallerThan(segmentId);
  }

  public long activeSegment() {
    return currentSegment;
  }

  public boolean appendNewSegment() {
    segmentLock.exclusiveLock();
    try {
      //noinspection NonAtomicOperationOnVolatileField
      currentSegment++;
      segmentSize.set(0);

      logMilestoneRecord();

      segmentAdditionTs = System.nanoTime();
    } finally {
      segmentLock.exclusiveUnlock();
    }

    return true;
  }

  public void appendSegment(final long segmentIndex) {
    if (segmentIndex <= currentSegment) {
      return;
    }

    segmentLock.exclusiveLock();
    try {
      if (segmentIndex <= currentSegment) {
        return;
      }

      currentSegment = segmentIndex;
      segmentSize.set(0);

      logMilestoneRecord();

      segmentAdditionTs = System.nanoTime();
    } finally {
      segmentLock.exclusiveUnlock();
    }
  }

  public List<String> getWalFiles() {
    final List<String> result = new ArrayList<>();

    for (final long segment : segments) {
      final String segmentName = getSegmentName(segment);
      final Path segmentPath = walLocation.resolve(segmentName);

      if (Files.exists(segmentPath)) {
        result.add(segmentPath.toAbsolutePath().toString());
      }
    }

    return result;
  }

  public Path getWMRFile() {
    return masterRecordPath;
  }

  public void moveLsnAfter(final OLogSequenceNumber lsn) {
    final long segment = lsn.getSegment() + 1;
    appendSegment(segment);
  }

  public long[] nonActiveSegments() {
    final OLogSequenceNumber writtenUpTo = this.writtenUpTo.get().lsn;

    long maxSegment = currentSegment;

    if (writtenUpTo.getSegment() < maxSegment) {
      maxSegment = writtenUpTo.getSegment();
    }

    final List<Long> result = new ArrayList<>();
    for (final long segment : segments) {
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

  public File[] nonActiveSegments(final long fromSegment) {
    final long maxSegment = currentSegment;
    final List<File> result = new ArrayList<>(8);

    for (final long segment : segments.tailSet(fromSegment)) {
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

  private OLogSequenceNumber doLogRecord(final OWriteableWALRecord writeableRecord) {
    if (writeableRecord.getBinaryContentLen() < 0) {
      final OPair<ByteBuffer, Long> serializedRecord = OWALRecordsFactory.toStream(writeableRecord);
      writeableRecord.setBinaryContent(serializedRecord.key, serializedRecord.value);
    }

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
    doFlush(true);
    waitTillWriteWillBeFinished();
  }

  public void close() throws IOException {
    close(true);
  }

  public void close(final boolean flush) throws IOException {
    if (flush) {
      doFlush(true);
    }

    stopWrite = true;

    if (recordsWriterFuture != null) {
      try {
        recordsWriterFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during writing of WAL records in storage " + storageName), e);
      }
    }

    if (writeFuture != null) {
      try {
        writeFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during writing of WAL records in storage " + storageName), e);
      }
    }

    OWALRecord record = records.poll();
    while (record != null) {
      if (record instanceof OWriteableWALRecord) {
        ((OWriteableWALRecord) record).freeBinaryContent();
      }

      record = records.poll();
    }

    try {
      if (writeFuture != null) {
        writeFuture.get();
      }

    } catch (final InterruptedException e) {
      OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
    } catch (final ExecutionException e) {
      OLogManager.instance().errorNoDb(this, "Error during writint of WAL data", e);
      throw OException.wrapException(new OStorageException("Error during writint of WAL data"), e);
    }

    for (final OPair<Long, OWALFile> pair : fileCloseQueue) {
      final OWALFile file = pair.value;

      if (callFsync) {
        file.force(true);
      }

      file.close();
    }

    fileCloseQueueSize.set(0);

    if (callFsync) {
      walFile.force(true);
    }

    walFile.close();
    masterRecordLSNHolder.close();
    segments.clear();
    fileCloseQueue.clear();

    allocator.deallocate(writeBufferPointerOne);
    allocator.deallocate(writeBufferPointerTwo);

    if (writeBufferPointer != null) {
      writeBufferPointer = null;
      writeBuffer = null;
      writeBufferPageIndex = -1;
    }
  }

  private void checkFreeSpace() throws IOException {
    final long freeSpace = fileStore.getUsableSpace();

    //system has unlimited amount of free space
    if (freeSpace < 0)
      return;

    if (walSizeHardLimit < 0 && freeSpace > freeSpaceLimit) {
      //(free space occupied by WAL + the rest of free space) / 2
      //so if WAL is empty we will consume half of free space provided to database
      walSizeLimit = (logSize.get() + freeSpace) / 2;
    }

    if (freeSpace < freeSpaceLimit) {
      for (final OLowDiskSpaceListener listener : lowDiskSpaceListeners) {
        listener.lowDiskSpace(new OLowDiskSpaceInformation(freeSpace, freeSpaceLimit));
      }
    }
  }

  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    lowDiskSpaceListeners.add(listener);
  }

  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
    final List<OLowDiskSpaceListener> itemsToRemove = new ArrayList<>();

    for (final OLowDiskSpaceListener lowDiskSpaceListener : lowDiskSpaceListeners) {
      if (lowDiskSpaceListener.equals(listener)) {
        itemsToRemove.add(lowDiskSpaceListener);
      }
    }

    lowDiskSpaceListeners.removeAll(itemsToRemove);
  }

  public void addFullCheckpointListener(final OCheckpointRequestListener listener) {
    fullCheckpointListeners.add(listener);
  }

  public void removeFullCheckpointListener(final OCheckpointRequestListener listener) {
    final List<OCheckpointRequestListener> itemsToRemove = new ArrayList<>();

    for (final OCheckpointRequestListener fullCheckpointRequestListener : fullCheckpointListeners) {
      if (fullCheckpointRequestListener.equals(listener)) {
        itemsToRemove.add(fullCheckpointRequestListener);
      }
    }

    fullCheckpointListeners.removeAll(itemsToRemove);
  }

  public void addSegmentOverflowListener(final OSegmentOverflowListener listener) {
    segmentOverflowListeners.add(listener);
  }

  private void doFlush(final boolean forceSync) {
    final Future<?> future = commitExecutor.submit(new RecordsWriter(forceSync, true, false));
    try {
      future.get();
    } catch (final Exception e) {
      OLogManager.instance().errorNoDb(this, "Exception during WAL flush", e);
      throw new IllegalStateException(e);
    }
  }

  public OLogSequenceNumber getFlushedLsn() {
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

      final Cursor<OWALRecord> nextCursor = MPSCFAAArrayDequeue.prev(cursor);
      if (nextCursor == null && record.getLsn().getPosition() < 0) {
        OLogManager.instance().warn(this, cursor.toString());
        throw new IllegalStateException("Invalid last record");
      }

      cursor = nextCursor;
    }

    if (!unassignedList.isEmpty()) {
      final ListIterator<OWALRecord> unassignedRecordsIterator = unassignedList.listIterator(unassignedList.size());

      OWALRecord prevRecord = unassignedRecordsIterator.previous();
      final OLogSequenceNumber prevLSN = prevRecord.getLsn();

      assert prevLSN.getPosition() >= 0;

      while (unassignedRecordsIterator.hasPrevious()) {
        final OWALRecord record = unassignedRecordsIterator.previous();
        final OLogSequenceNumber lsn = record.getLsn();

        if (lsn.getPosition() < 0) {
          final long position = calculatePosition(record, prevRecord, pageSize, maxRecordSize);
          final OLogSequenceNumber newLSN = new OLogSequenceNumber(lsn.getSegment(), position);

          if (record.getLsn().getPosition() < 0) {
            record.setLsn(newLSN);
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

  private static long calculatePosition(final OWALRecord record, final OWALRecord prevRecord, int pageSize, int maxRecordSize) {
    assert
        prevRecord.getLsn().getSegment() <= record.getLsn().getSegment() :
        "prev segment " + prevRecord.getLsn().getSegment() + " segment " + record.getLsn().getSegment();

    if (prevRecord instanceof OStartWALRecord) {
      assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();

      if (record instanceof OMilestoneWALRecord) {
        record.setDistance(0);
        record.setDiskSize(prevRecord.getDiskSize());
      } else {
        final int recordLength = ((OWriteableWALRecord) record).getBinaryContentLen();
        final int length = OCASWALPage.calculateSerializedSize(recordLength);

        final int pages = length / maxRecordSize;
        final int offset = length - pages * maxRecordSize;

        final int distance;
        if (pages == 0) {
          distance = length;
        } else {
          distance = (pages - 1) * pageSize + offset + maxRecordSize + OCASWALPage.RECORDS_OFFSET;
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
        final int recordLength = ((OWriteableWALRecord) record).getBinaryContentLen();
        final int length = OCASWALPage.calculateSerializedSize(recordLength);

        final int pages = length / maxRecordSize;
        final int offset = length - pages * maxRecordSize;

        final int distance;
        if (pages == 0) {
          distance = length;
        } else {
          distance = (pages - 1) * pageSize + offset + maxRecordSize + OCASWALPage.RECORDS_OFFSET;
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
        final long pageIndex = end / pageSize;

        final long newPosition;
        final int pageOffset = (int) (end - pageIndex * pageSize);

        if (pageOffset > OCASWALPage.RECORDS_OFFSET) {
          newPosition = (pageIndex + 1) * pageSize + OCASWALPage.RECORDS_OFFSET;
          record.setDiskSize((int) ((pageIndex + 1) * pageSize - end) + OCASWALPage.RECORDS_OFFSET);
        } else {
          newPosition = end;
          record.setDiskSize(OCASWALPage.RECORDS_OFFSET);
        }

        record.setDistance(0);

        return newPosition;
      } else {
        final long prevPosition = prevRecord.getLsn().getPosition();
        final long end = prevPosition + prevRecord.getDistance();
        final long pageIndex = end / pageSize;
        final int pageOffset = (int) (end - pageIndex * pageSize);

        if (pageOffset == OCASWALPage.RECORDS_OFFSET) {
          record.setDiskSize(OCASWALPage.RECORDS_OFFSET);
        } else {
          final int pageFreeSpace = pageSize - pageOffset;
          record.setDiskSize(pageFreeSpace + OCASWALPage.RECORDS_OFFSET);
        }

        record.setDistance(0);

        return OCASWALPage.RECORDS_OFFSET;
      }
    }

    assert prevRecord.getLsn().getSegment() == record.getLsn().getSegment();
    final long start = prevRecord.getDistance() + prevRecord.getLsn().getPosition();
    final int freeSpace = pageSize - (int) (start % pageSize);
    final int startOffset = pageSize - freeSpace;

    final int recordLength = ((OWriteableWALRecord) record).getBinaryContentLen();
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

      @SuppressWarnings("UnnecessaryLocalVariable")
      final int firstChunk = freeSpace;
      final int pages = length / maxRecordSize;
      final int offset = length - pages * maxRecordSize;

      final int distance = firstChunk + pages * pageSize + offset + OCASWALPage.RECORDS_OFFSET;
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

  private void fireEventsFor(final OLogSequenceNumber lsn) {
    // may be executed by only one thread at every instant of time

    final Iterator<Runnable> eventsToFire = events.headMap(lsn, true).values().iterator();
    while (eventsToFire.hasNext()) {
      eventsToFire.next().run();
      eventsToFire.remove();
    }
  }

  private String getSegmentName(final long segment) {
    return storageName + "." + segment + WAL_SEGMENT_EXTENSION;
  }

  private final class RecordsWriter implements Runnable {
    private final boolean forceSync;
    private final boolean fullWrite;
    private final boolean reschedule;

    private RecordsWriter(final boolean forceSync, final boolean fullWrite, boolean reschedule) {
      this.forceSync = forceSync;
      this.fullWrite = fullWrite;
      this.reschedule = reschedule;
    }

    @Override
    public void run() {
      if (stopWrite) {
        return;
      }
      try {
        if (printPerformanceStatistic) {
          printReport();
        }

        final long ts = System.nanoTime();
        final boolean makeFSync = forceSync || ts - lastFSyncTs > fsyncInterval * 1_000_000;
        final long qSize = queueSize.get();

        //even if queue is empty we need to write buffer content to the disk if needed
        if (qSize > 0 || fullWrite || makeFSync) {
          final CountDownLatch fl = new CountDownLatch(1);
          flushLatch.lazySet(fl);
          try {
            //in case of "full write" mode, we log milestone record and iterate over the queue till we find it
            final OMilestoneWALRecord milestoneRecord;
            //in case of "full cache" mode we chose last record in the queue, iterate till this record and write it if needed
            //but do not remove this record from the queue, so we will always have queue with record with valid LSN
            //if we write last record, we mark it as written, so we do not repeat that again
            final OWALRecord lastRecord;

            //we jump to new page if we need to make fsync or we need to be sure that records are written in file system
            if (makeFSync || fullWrite) {
              segmentLock.sharedLock();
              try {
                milestoneRecord = logMilestoneRecord();
              } finally {
                segmentLock.sharedUnlock();
              }

              lastRecord = null;
            } else {

              final Cursor<OWALRecord> cursor = records.peekLast();
              assert cursor != null;

              lastRecord = cursor.getItem();
              assert lastRecord != null;

              if (lastRecord.getLsn().getPosition() == -1) {
                calculateRecordsLSNs();
              }

              assert lastRecord.getLsn().getPosition() >= 0;
              milestoneRecord = null;
            }

            while (true) {
              final OWALRecord record = records.peek();

              if (record == milestoneRecord) {
                break;
              }

              assert record != null;
              final OLogSequenceNumber lsn = record.getLsn();

              assert lsn.getSegment() >= segmentId;

              if (!(record instanceof OMilestoneWALRecord) && !(record instanceof OStartWALRecord)) {
                if (segmentId != lsn.getSegment()) {
                  if (walFile != null) {
                    if (writeBufferPointer != null) {
                      writeBuffer(walFile, writeBuffer, lastLSN, checkPointLSN);
                    }

                    writeBufferPointer = null;
                    writeBuffer = null;
                    writeBufferPageIndex = -1;

                    checkPointLSN = null;
                    lastLSN = null;

                    try {
                      if (writeFuture != null) {
                        writeFuture.get();
                      }
                    } catch (final InterruptedException e) {
                      OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
                    }

                    assert walFile.position() == currentPosition;

                    fileCloseQueueSize.incrementAndGet();
                    fileCloseQueue.offer(new OPair<>(segmentId, walFile));
                  }

                  segmentId = lsn.getSegment();

                  walFile = OWALFile.createWriteWALFile(walLocation.resolve(getSegmentName(segmentId)), allowDirectIO, blockSize);
                  assert lsn.getPosition() == OCASWALPage.RECORDS_OFFSET;
                  currentPosition = 0;
                }

                final OWriteableWALRecord writeableRecord = (OWriteableWALRecord) record;

                if (!writeableRecord.isWritten()) {
                  int written = 0;
                  final int recordContentBinarySize = writeableRecord.getBinaryContentLen();
                  final int bytesToWrite = OIntegerSerializer.INT_SIZE + recordContentBinarySize;

                  final ByteBuffer recordContent = writeableRecord.getBinaryContent();
                  recordContent.position(0);

                  byte[] recordSize = null;
                  int recordSizeWritten = -1;

                  boolean recordSizeIsWritten = false;

                  while (written < bytesToWrite) {
                    if (writeBuffer == null || writeBuffer.remaining() == 0) {
                      if (writeBufferPointer != null) {
                        assert writeBuffer != null;
                        writeBuffer(walFile, writeBuffer, lastLSN, checkPointLSN);
                      }

                      if (useFirstBuffer) {
                        writeBufferPointer = writeBufferPointerOne;
                        writeBuffer = writeBufferOne;
                      } else {
                        writeBufferPointer = writeBufferPointerTwo;
                        writeBuffer = writeBufferTwo;
                      }

                      writeBuffer.limit(writeBuffer.capacity());
                      writeBuffer.rewind();
                      useFirstBuffer = !useFirstBuffer;

                      writeBufferPageIndex = -1;

                      checkPointLSN = null;
                      lastLSN = null;
                    }

                    if (writeBuffer.position() % pageSize == 0) {
                      writeBufferPageIndex++;
                      writeBuffer.position(writeBuffer.position() + OCASWALPage.RECORDS_OFFSET);
                    }

                    assert
                        written != 0 || currentPosition + writeBuffer.position() == lsn.getPosition() :
                        (currentPosition + writeBuffer.position()) + " vs " + lsn.getPosition();
                    final int chunkSize = Math
                        .min(bytesToWrite - written, (writeBufferPageIndex + 1) * pageSize - writeBuffer.position());
                    assert chunkSize <= maxRecordSize;
                    assert chunkSize + writeBuffer.position() <= (writeBufferPageIndex + 1) * pageSize;
                    assert writeBuffer.position() > writeBufferPageIndex * pageSize;

                    if (!recordSizeIsWritten) {
                      if (recordSizeWritten > 0) {
                        writeBuffer.put(recordSize, recordSizeWritten, OIntegerSerializer.INT_SIZE - recordSizeWritten);
                        written += OIntegerSerializer.INT_SIZE - recordSizeWritten;

                        recordSize = null;
                        recordSizeWritten = -1;
                        recordSizeIsWritten = true;
                        continue;
                      } else if (OIntegerSerializer.INT_SIZE <= chunkSize) {
                        writeBuffer.putInt(recordContentBinarySize);
                        written += OIntegerSerializer.INT_SIZE;

                        recordSize = null;
                        recordSizeWritten = -1;
                        recordSizeIsWritten = true;
                        continue;
                      } else {
                        recordSize = new byte[OIntegerSerializer.INT_SIZE];
                        OIntegerSerializer.INSTANCE.serializeNative(recordContentBinarySize, recordSize, 0);

                        recordSizeWritten = (writeBufferPageIndex + 1) * pageSize - writeBuffer.position();
                        written += recordSizeWritten;

                        writeBuffer.put(recordSize, 0, recordSizeWritten);
                        continue;
                      }
                    }

                    recordContent.limit(recordContent.position() + chunkSize);
                    writeBuffer.put(recordContent);
                    written += chunkSize;
                  }

                  lastLSN = lsn;
                  if (writeableRecord.isUpdateMasterRecord()) {
                    checkPointLSN = lastLSN;
                  }

                  queueSize.addAndGet(-writeableRecord.getDiskSize());
                  writeableRecord.written();
                  writeableRecord.freeBinaryContent();
                }

              }
              if (lastRecord != record) {
                records.poll();
              } else {
                break;
              }
            }

            if ((makeFSync || fullWrite) && writeBufferPointer != null) {
              writeBuffer(walFile, writeBuffer, lastLSN, checkPointLSN);

              writeBufferPointer = null;
              writeBuffer = null;
              writeBufferPageIndex = -1;

              checkPointLSN = null;
              lastLSN = null;
            }
          } finally {
            fl.countDown();
          }

          if (qSize > 0 && ts - segmentAdditionTs >= segmentsInterval) {
            for (final OSegmentOverflowListener listener : segmentOverflowListeners) {
              listener.onSegmentOverflow(currentSegment);
            }
          }
        }

        if (makeFSync) {
          try {
            try {
              if (writeFuture != null) {
                writeFuture.get();
              }
            } catch (final InterruptedException e) {
              OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
            }

            assert walFile.position() == currentPosition;

            writeFuture = writeExecutor.submit((Callable<?>) () -> {
              try {
                long startTs = 0;
                if (printPerformanceStatistic) {
                  startTs = System.nanoTime();
                }

                final int cqSize = fileCloseQueueSize.get();
                if (cqSize > 0) {
                  int counter = 0;

                  while (counter < cqSize) {
                    final OPair<Long, OWALFile> pair = fileCloseQueue.poll();
                    if (pair != null) {
                      @SuppressWarnings("resource")
                      final OWALFile file = pair.value;

                      assert file.position() % pageSize == 0;

                      if (callFsync) {
                        file.force(true);
                      }

                      file.close();

                      fileCloseQueueSize.decrementAndGet();
                    } else {
                      break;
                    }

                    counter++;
                  }
                }

                if (callFsync) {
                  walFile.force(true);
                }

                updateCheckpoint(writtenCheckpoint);
                flushedLSN = writtenUpTo.get().lsn;

                fireEventsFor(flushedLSN);

                if (printPerformanceStatistic) {
                  final long endTs = System.nanoTime();
                  //noinspection NonAtomicOperationOnVolatileField
                  fsyncTime += (endTs - startTs);
                  //noinspection NonAtomicOperationOnVolatileField
                  fsyncCount++;
                }
              } catch (final IOException e) {
                OLogManager.instance().errorNoDb(this, "Error during FSync of WAL data", e);
                throw e;
              }

              return null;
            });

            checkFreeSpace();
          } finally {
            lastFSyncTs = ts;
          }
        }
      } catch (final IOException | ExecutionException e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);
        throw new IllegalStateException(e);
      } catch (final RuntimeException | Error e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL writing", e);
        throw e;
      } finally {
        if (reschedule && !stopWrite) {
          recordsWriterFuture = commitExecutor.schedule(this, commitDelay, TimeUnit.MILLISECONDS);
        }
      }
    }

    private void writeBuffer(final OWALFile file, final ByteBuffer buffer, final OLogSequenceNumber lastLSN,
        final OLogSequenceNumber checkpointLSN) throws IOException {

      if (buffer.position() <= OCASWALPage.RECORDS_OFFSET) {
        return;
      }

      int maxPage = (buffer.position() + pageSize - 1) / pageSize;
      int lastPageSize = buffer.position() - (maxPage - 1) * pageSize;

      if (lastPageSize <= OCASWALPage.RECORDS_OFFSET) {
        maxPage--;
        lastPageSize = pageSize;
      }

      for (int start = 0, page = 0; start < maxPage * pageSize; start += pageSize, page++) {
        final int pageSize;
        if (page < maxPage - 1) {
          pageSize = OCASDiskWriteAheadLog.this.pageSize;
        } else {
          pageSize = lastPageSize;
        }

        buffer.limit(start + pageSize);

        buffer.position(start + OCASWALPage.MAGIC_NUMBER_OFFSET);
        buffer.putLong(OCASWALPage.MAGIC_NUMBER);

        buffer.position(start + OCASWALPage.PAGE_SIZE_OFFSET);
        buffer.putShort((short) pageSize);

        buffer.position(start + OCASWALPage.RECORDS_OFFSET);
        final XXHash64 xxHash64 = xxHashFactory.hash64();
        final long hash = xxHash64.hash(buffer, XX_SEED);

        buffer.position(start + OCASWALPage.XX_OFFSET);
        buffer.putLong(hash);
      }

      buffer.position(0);
      final int limit = maxPage * pageSize;
      buffer.limit(limit);

      try {
        if (writeFuture != null) {
          writeFuture.get();
        }
      } catch (final InterruptedException e) {
        OLogManager.instance().errorNoDb(this, "WAL write was interrupted", e);
      } catch (final Exception e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL write", e);
        throw OException.wrapException(new OStorageException("Error during WAL data write"), e);
      }

      assert file.position() == currentPosition;
      currentPosition += buffer.limit();

      final long expectedPosition = currentPosition;

      writeFuture = writeExecutor.submit((Callable<?>) () -> {
        try {
          long startTs = 0;
          if (printPerformanceStatistic) {
            startTs = System.nanoTime();
          }

          assert buffer.position() == 0;
          assert file.position() % pageSize == 0;
          assert buffer.limit() == limit;

          while (buffer.remaining() > 0) {
            final int initialPos = buffer.position();
            final int written = file.write(buffer);
            assert buffer.position() == initialPos + written;
          }

          assert file.position() == expectedPosition;

          if (lastLSN != null) {
            final WrittenUpTo written = writtenUpTo.get();

            assert written == null || written.lsn.compareTo(lastLSN) < 0;

            if (written == null) {
              writtenUpTo.lazySet(new WrittenUpTo(lastLSN, buffer.limit()));
            } else {
              if (written.lsn.getSegment() == lastLSN.getSegment()) {
                writtenUpTo.lazySet(new WrittenUpTo(lastLSN, written.position + buffer.limit()));
              } else {
                writtenUpTo.lazySet(new WrittenUpTo(lastLSN, buffer.limit()));
              }
            }
          }

          if (checkpointLSN != null) {
            assert writtenCheckpoint == null || writtenCheckpoint.compareTo(checkpointLSN) < 0;
            writtenCheckpoint = checkpointLSN;
          }

          if (printPerformanceStatistic) {
            final long endTs = System.nanoTime();

            //noinspection NonAtomicOperationOnVolatileField
            bytesWrittenSum += buffer.limit();
            //noinspection NonAtomicOperationOnVolatileField
            bytesWrittenTime += (endTs - startTs);
          }
        } catch (final IOException e) {
          OLogManager.instance().errorNoDb(this, "Error during WAL data write", e);
          throw e;
        }

        return null;
      });
    }

    private void printReport() {
      final long ts = System.nanoTime();
      final long reportInterval;

      if (reportTs == -1) {
        reportTs = ts;
        reportInterval = 0;
      } else {
        reportInterval = ts - reportTs;
      }

      if (reportInterval >= statisticPrintInterval * 1_000_000_000L) {
        final long bytesWritten = OCASDiskWriteAheadLog.this.bytesWrittenSum;
        final long writtenTime = OCASDiskWriteAheadLog.this.bytesWrittenTime;

        final long fsyncTime = OCASDiskWriteAheadLog.this.fsyncTime;
        final long fsyncCount = OCASDiskWriteAheadLog.this.fsyncCount;

        final long threadsWaitingCount = OCASDiskWriteAheadLog.this.threadsWaitingCount.sum();
        final long threadsWaitingSum = OCASDiskWriteAheadLog.this.threadsWaitingSum.sum();

        OLogManager.instance().infoNoDb(this, "WAL stat:%s: %d KB was written, write speed is %d KB/s. FSync count %d. "
                + "Avg. fsync time %d ms. %d times threads were waiting for WAL. Avg wait interval %d ms.", storageName,
            bytesWritten / 1024, writtenTime > 0 ? 1_000_000_000L * bytesWritten / writtenTime / 1024 : -1, fsyncCount,
            fsyncCount > 0 ? fsyncTime / fsyncCount / 1_000_000 : -1, threadsWaitingCount,
            threadsWaitingCount > 0 ? threadsWaitingSum / threadsWaitingCount / 1_000_000 : -1);

        //noinspection NonAtomicOperationOnVolatileField
        OCASDiskWriteAheadLog.this.bytesWrittenSum -= bytesWritten;
        //noinspection NonAtomicOperationOnVolatileField
        OCASDiskWriteAheadLog.this.bytesWrittenTime -= writtenTime;

        //noinspection NonAtomicOperationOnVolatileField
        OCASDiskWriteAheadLog.this.fsyncTime -= fsyncTime;
        //noinspection NonAtomicOperationOnVolatileField
        OCASDiskWriteAheadLog.this.fsyncCount -= fsyncCount;

        OCASDiskWriteAheadLog.this.threadsWaitingSum.add(-threadsWaitingSum);
        OCASDiskWriteAheadLog.this.threadsWaitingCount.add(-threadsWaitingCount);

        reportTs = ts;
      }

    }

  }

  private static final class WrittenUpTo {
    private final OLogSequenceNumber lsn;
    private final long               position;

    WrittenUpTo(final OLogSequenceNumber lsn, final long position) {
      this.lsn = lsn;
      this.position = position;
    }
  }
}
