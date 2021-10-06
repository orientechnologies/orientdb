package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import jnr.a64asm.OP;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public final class MMAPWriteAheadLog implements OWriteAheadLog {
  public static final String DEFAULT_WAL_SEGMENT_EXTENSION = ".wal";

  private final String fileExtension;
  private final String storageName;
  private final Path walPath;

  private final int segmentSize;

  private volatile ModifiableWALSegment activeSegment;
  private final ReentrantLock activeSegmentLock = new ReentrantLock();

  private final ScalableRWLock stateLock = new ScalableRWLock();
  private boolean closed = false;

  private final ConcurrentSkipListMap<Long, WALSegment> segments = new ConcurrentSkipListMap<>();
  private final ConcurrentLinkedQueue<ModifiableWALSegment> flushingQueue =
      new ConcurrentLinkedQueue<>();

  private final AtomicReference<OLogSequenceNumber> flushedLSN = new AtomicReference<>();

  public MMAPWriteAheadLog(
      String fileExtension, String storageName, Path walPath, int segmentSize) {
    this.fileExtension = fileExtension;
    this.storageName = storageName;
    this.walPath = walPath;
    this.segmentSize = segmentSize;
  }

  /** Fetches all existing WAL segments before starting to work with them. */
  public void init(final boolean exceptionOnError) throws IOException {
    //1. Load all segments.
    //2. Check which segments are not broken.
    //3. Keep only healthy segments plus first broken segment which contains at least one record.
    //4. All segment indexes should grow continuously if there is a gap all indexes after the gap are removed too.

    stateLock.exclusiveLock();
    try {
      try (final Stream<Path> walFiles =
          Files.find(
              walPath,
              1,
              (path, basicFileAttributes) -> path.getFileName().endsWith(fileExtension))) {
        final Iterator<Path> filesIterator = walFiles.iterator();

        while (filesIterator.hasNext()) {
          final Path walPath = filesIterator.next();
          try {
            final WALSegment walSegment = new ReadOnlyWALSegment(walPath);
            final Optional<Long> segmentIndex = walSegment.segmentIndex();
            if (!segmentIndex.isPresent()) {
              final String message = "WAL segment is broken and can not be used";
              OLogManager.instance().errorNoDb(this, message, null);
              if (exceptionOnError) {
                throw new IllegalStateException(message);
              }
              break;
            }

            final WALSegment prevSegment = segments.putIfAbsent(segmentIndex.get(), walSegment);
            if (prevSegment != null) {
              OLogManager.instance()
                  .errorNoDb(
                      this,
                      "Error during reading of WAL segment. WAL contains two segments with the same id - "
                          + segmentIndex.get(),
                      null);
              if (exceptionOnError) {
                throw new IllegalStateException(
                    "Error during reading of WAL segment. WAL contains two segments with the same id - "
                        + segmentIndex.get());
              }
              break;
            }
          } catch (final IOException e) {
            OLogManager.instance().errorNoDb(this, "Error during reading of WAL segment", e);
            if (exceptionOnError) {
              throw new IllegalStateException("Error during reading of WAL segment", e);
            }
            break;
          }
        }
      }

      final long activeSegmentIndex;
      if (segments.isEmpty()) {
        activeSegmentIndex = 1;
      } else {
        activeSegmentIndex = segments.lastKey() + 1;
      }

      flushedLSN.set(end());

      activeSegment =
          new ModifiableWALSegment(
              walPath.resolve(getSegmentName(storageName, activeSegmentIndex, fileExtension)),
              segmentSize,
              activeSegmentIndex);
      segments.put(activeSegmentIndex, activeSegment);

    } finally {
      stateLock.exclusiveUnlock();
    }
  }

  private String getSegmentName(
      final String storageName, final long segment, final String fileExtension) {
    return storageName + "." + segment + fileExtension;
  }

  @Override
  public OLogSequenceNumber begin() throws IOException {
    return begin(0);
  }

  @Override
  public OLogSequenceNumber begin(long segmentId) throws IOException {
    stateLock.sharedLock();
    try {
      for (final WALSegment walSegment : segments.tailMap(segmentId, true).values()) {
        final Optional<OLogSequenceNumber> begin = walSegment.begin();
        if (begin.isPresent()) {
          return begin.get();
        }
      }

      return null;
    } finally {
      stateLock.sharedUnlock();
    }
  }

  @Override
  public OLogSequenceNumber end() {
    for (final WALSegment walSegment : segments.descendingMap().values()) {
      final Optional<OLogSequenceNumber> end = walSegment.end();
      if (end.isPresent()) {
        return end.get();
      }
    }

    return null;
  }

  @Override
  public void flush() {
    OLogSequenceNumber end = null;

    for (final WALSegment walSegment : segments.values()) {
      if (walSegment instanceof ModifiableWALSegment) {
        final ModifiableWALSegment modifiableWALSegment = (ModifiableWALSegment) walSegment;
        modifiableWALSegment.sync();
      }

      final Optional<OLogSequenceNumber> sEnd = walSegment.end();
      if (sEnd.isPresent()) {
        end = sEnd.get();
      }
    }

    if (end != null) {
      while (true) {
        final OLogSequenceNumber flushedLSN = this.flushedLSN.get();

        if (flushedLSN == null || end.compareTo(flushedLSN) > 0) {
          if (this.flushedLSN.compareAndSet(flushedLSN, end)) {
            break;
          }
        }
      }
    }
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(
      boolean isRollbackSupported, long unitId, byte[] metadata) {
    return null;
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, long unitId)
      throws IOException {
    return null;
  }

  @Override
  public OLogSequenceNumber logAtomicOperationEndRecord(
      long operationUnitId,
      boolean rollback,
      OLogSequenceNumber startLsn,
      Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata)
      throws IOException {
    return null;
  }

  @Override
  public OLogSequenceNumber log(WriteableWALRecord record) throws IOException {
    stateLock.sharedLock();
    try {
      ModifiableWALSegment segment = this.activeSegment;
      Optional<OLogSequenceNumber> lsn = segment.write(record);

      if (lsn.isPresent()) {
        return lsn.get();
      }

      while (true) {
        activeSegmentLock.lock();
        try {
          segment = this.activeSegment;

          lsn = segment.write(record);

          if (lsn.isPresent()) {
            return lsn.get();
          }

          final long nextSegmentIndex =
              this.activeSegment
                      .segmentIndex()
                      .orElseThrow(() -> new OStorageException("Active WAL segment is broken"))
                  + 1;

          final Path nextSegmentPath =
              walPath.resolve(getSegmentName(storageName, nextSegmentIndex, fileExtension));
          final ModifiableWALSegment prevSegment = segment;

          segment = new ModifiableWALSegment(nextSegmentPath, segmentSize, nextSegmentIndex);

          // do not change the order to avoid MT issues
          segments.put(nextSegmentIndex, segment);
          this.activeSegment = segment;
          flushingQueue.add(prevSegment);

        } finally {
          activeSegmentLock.unlock();
        }

        lsn = segment.write(record);

        if (lsn.isPresent()) {
          return lsn.get();
        }
      }
    } finally {
      stateLock.sharedUnlock();
    }
  }

  @Override
  public int lastOperationId() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  @Override
  public void close(boolean flush) throws IOException {
    stateLock.exclusiveLock();
    try {
      for (final WALSegment segment : segments.values()) {
        if (flush && segment instanceof ModifiableWALSegment) {
          final ModifiableWALSegment modifiableWALSegment = (ModifiableWALSegment) segment;
          modifiableWALSegment.sync();
        }

        try {
          segment.close();
        } catch (Exception e) {
          throw new IOException(e);
        }

        closed = true;
      }
    } finally {
      stateLock.exclusiveUnlock();
    }
  }

  @Override
  public void delete() throws IOException {
    stateLock.exclusiveLock();
    try {
      for (final WALSegment segment : segments.values()) {
        segment.delete();
      }

      closed = true;
    } finally {
      stateLock.exclusiveUnlock();
    }
  }

  @Override
  public List<WriteableWALRecord> read(OLogSequenceNumber lsn, int limit) throws IOException {
    return null;
  }

  @Override
  public List<WriteableWALRecord> next(OLogSequenceNumber lsn, int limit) throws IOException {
    return null;
  }

  @Override
  public OLogSequenceNumber getFlushedLsn() {
    return null;
  }

  @Override
  public boolean cutTill(OLogSequenceNumber lsn) throws IOException {
    return false;
  }

  @Override
  public boolean cutAllSegmentsSmallerThan(long segmentId) throws IOException {
    return false;
  }

  @Override
  public void addCheckpointListener(OCheckpointRequestListener listener) {}

  @Override
  public void removeCheckpointListener(OCheckpointRequestListener listener) {}

  @Override
  public void moveLsnAfter(OLogSequenceNumber lsn) throws IOException {}

  @Override
  public void addCutTillLimit(OLogSequenceNumber lsn) {}

  @Override
  public void removeCutTillLimit(OLogSequenceNumber lsn) {}

  @Override
  public File[] nonActiveSegments(long fromSegment) {
    return new File[0];
  }

  @Override
  public long[] nonActiveSegments() {
    return new long[0];
  }

  @Override
  public long activeSegment() {
    return 0;
  }

  @Override
  public void addEventAt(OLogSequenceNumber lsn, Runnable event) {}

  @Override
  public boolean appendNewSegment() {
    return false;
  }
}
