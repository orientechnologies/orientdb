package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.agrona.IoUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import java.util.concurrent.atomic.AtomicLong;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap.ReadOnlyWALSegment.*;

public final class ModifiableWALSegment implements WALSegment {
  private static final XXHash64 xxHash = XXHashFactory.fastestInstance().hash64();
  private final Path segmentPath;

  private final AtomicLong lastPositionFreeHeader = new AtomicLong();

  private final ScalableRWLock syncLock = new ScalableRWLock();

  private final MappedByteBuffer buffer;
  private final ByteBuffer dataBuffer;

  private final long segmentIndex;

  private boolean closed;

  private volatile boolean dirty = false;

  public ModifiableWALSegment(
      final Path segmentPath, final int segmentSize, final long segmentIndex) throws IOException {
    syncLock.exclusiveLock();
    try {
      this.segmentPath = segmentPath;
      this.segmentIndex = segmentIndex;

      try (final FileChannel fileChannel =
          FileChannel.open(
              segmentPath,
              StandardOpenOption.READ,
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE)) {
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize);
      }

      buffer.putLong(segmentIndex);
      final long hash = xxHash.hash(buffer, 0, SEGMENT_INDEX_SIZE, XX_HASH_SEED);
      buffer.putLong(hash);

      buffer.force();

      buffer.position(METADATA_SIZE);

      dataBuffer = buffer.slice();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  public Optional<OLogSequenceNumber> write(final WriteableWALRecord record) {
    syncLock.sharedLock();
    try {
      checkForClose();

      // optimization to decrease amount of membars during write
      if (!dirty) {
        dirty = true;
      }

      final int recordSize = record.serializedSize();
      if (recordSize + RECORD_SYSTEM_DATA_SIZE > dataBuffer.capacity()) {
        throw new IllegalArgumentException(
            "Record size exceeds maximum allowed size of the record "
                + recordSize
                + " vs "
                + (dataBuffer.capacity() - RECORD_SYSTEM_DATA_SIZE));
      }

      while (true) {
        final long lastPositionFreeHeader = this.lastPositionFreeHeader.get();

        final int freeHeader = (int) (lastPositionFreeHeader >>> 32);

        final int delta = recordSize + RECORD_SYSTEM_DATA_SIZE;
        final int newFreeHeader = freeHeader + delta;
        @SuppressWarnings("UnnecessaryLocalVariable")
        final int newPosition = freeHeader;

        if (newFreeHeader <= dataBuffer.capacity()) {
          long newLastPositionFreeHeader = (((long) newFreeHeader) << (8 * 4)) | newPosition;
          if (this.lastPositionFreeHeader.compareAndSet(
              lastPositionFreeHeader, newLastPositionFreeHeader)) {
            final OLogSequenceNumber lsn = new OLogSequenceNumber(segmentIndex, newPosition);

            record.setOperationIdLsn(lsn, 0);
            final ByteBuffer copy = dataBuffer.duplicate();
            copy.position(freeHeader);

            copy.putShort((short) record.getId());
            copy.putInt(recordSize);
            record.toStream(copy);

            final long hash =
                xxHash.hash(
                    copy, freeHeader, recordSize + RECORD_SIZE_SIZE + RECORD_ID_SIZE, XX_HASH_SEED);

            copy.putLong(hash);

            return Optional.of(lsn);
          }
        } else {
          return Optional.empty();
        }
      }
    } finally {
      syncLock.sharedUnlock();
    }
  }

  @Override
  public Optional<WriteableWALRecord> read(final OLogSequenceNumber lsn) {
    // Initially we try to read record inside of shared lock,
    // but there could be a situation when record is only partially written or invalid LSN is
    // passed.
    // If record is partially written then hash code will not match to the record content and
    // exception will be
    // thrown. After that write lock will be acquired and WAL waits till writing is done.
    // If record is broken during the read once write lock is acquired that means that invalid value
    // of LSN is passed
    // and read is performed starting from the middle of the other record. We do not consider
    // situation when writable segment
    // could be broken because only new segments are used inside current running  session and never
    // read from disk.
    try {
      syncLock.sharedLock();
      try {
        checkForClose();

        return Optional.of(doRead(lsn));
      } finally {
        syncLock.sharedUnlock();
      }
    } catch (InvalidRecordReadException e) {
      syncLock.exclusiveLock();
      try {
        checkForClose();

        return Optional.of(doRead(lsn));
      } catch (InvalidRecordReadException irre) {
        throw OException.wrapException(
            new OStorageException("Invalid LSN is passed as method argument"), irre);
      } finally {
        syncLock.exclusiveUnlock();
      }
    }
  }

  private WriteableWALRecord doRead(OLogSequenceNumber lsn) throws InvalidRecordReadException {
    if (lsn.getSegment() != segmentIndex) {
      throw new IllegalArgumentException(
          "Segment index passed in LSN differs from current segment index. "
              + lsn.getSegment()
              + " vs. "
              + segmentIndex);
    }

    if (lsn.getPosition() >= dataBuffer.capacity()) {
      throw new IllegalArgumentException(
          "Requested record position "
              + lsn.getPosition()
              + " bigger than segment size "
              + dataBuffer.capacity());
    }

    final int position = (int) this.lastPositionFreeHeader.get();

    if (lsn.getPosition() > position) {
      throw new InvalidRecordReadException();
    }

    final ByteBuffer copy = dataBuffer.duplicate();
    copy.position(lsn.getPosition());

    final int recordId = copy.getShort();
    if (recordId < 0) {
      throw new InvalidRecordReadException();
    }

    final int recordSize = copy.getInt();
    if (recordSize < 0
        || lsn.getPosition() + recordSize + RECORD_SYSTEM_DATA_SIZE > dataBuffer.capacity()) {
      throw new InvalidRecordReadException();
    }

    final long hash =
        copy.getLong(lsn.getPosition() + RECORD_ID_SIZE + RECORD_SIZE_SIZE + recordSize);
    if (xxHash.hash(
            copy, lsn.getPosition(), RECORD_ID_SIZE + RECORD_SIZE_SIZE + recordSize, XX_HASH_SEED)
        != hash) {
      throw new InvalidRecordReadException();
    }

    final WriteableWALRecord record = OWALRecordsFactory.INSTANCE.walRecordById(recordId);
    record.fromStream(copy);

    record.setOperationIdLsn(lsn, 0);

    return record;
  }

  @Override
  public Optional<WriteableWALRecord> next(final WriteableWALRecord record) {
    final int serializedSize = record.serializedSize();
    final int nextPosition =
        record.getOperationIdLSN().lsn.getPosition() + serializedSize + RECORD_SYSTEM_DATA_SIZE;

    final int position = (int) this.lastPositionFreeHeader.get();
    if (nextPosition > position) {
      return Optional.empty();
    }

    return read(new OLogSequenceNumber(segmentIndex, nextPosition));
  }

  @Override
  public Optional<WriteableWALRecord> next(final OLogSequenceNumber lsn) {
    final Optional<WriteableWALRecord> record = read(lsn);

    return record.flatMap(this::next);
  }

  public void sync() {
    syncLock.exclusiveLock();
    try {
      checkForClose();

      doSync();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void doSync() {
    if (!dirty) {
      return;
    }

    buffer.position(SEGMENT_INDEX_SIZE + XX_HASH_SIZE);

    final long lastPositionFreeHeader = this.lastPositionFreeHeader.get();

    final int position = (int) lastPositionFreeHeader;
    final int freeHeader = (int) (lastPositionFreeHeader >>> 32);
    if (position != freeHeader) {
      buffer.putInt(0);
      buffer.putInt(position);
    } else {
      assert position == 0;

      buffer.putInt(-1);
      buffer.putInt(-1);
    }

    final long hash =
        xxHash.hash(
            buffer,
            SEGMENT_INDEX_SIZE + XX_HASH_SIZE,
            BEGIN_POSITION_SIZE + END_POSITION_SIZE,
            XX_HASH_SEED);
    buffer.putLong(hash);

    buffer.force();

    dirty = false;
  }

  @Override
  public Optional<OLogSequenceNumber> begin() {
    syncLock.sharedLock();
    try {
      checkForClose();

      final long lastPositionFreeHeader = this.lastPositionFreeHeader.get();

      final int position = (int) lastPositionFreeHeader;
      final int freeHeader = (int) (lastPositionFreeHeader >>> 32);

      if (position == freeHeader) {
        assert position == 0;

        return Optional.empty();
      }

      return Optional.of(new OLogSequenceNumber(segmentIndex, 0));
    } finally {
      syncLock.sharedUnlock();
    }
  }

  @Override
  public Optional<OLogSequenceNumber> end() {
    syncLock.sharedLock();
    try {
      checkForClose();

      final long lastPositionFreeHeader = this.lastPositionFreeHeader.get();

      final int position = (int) lastPositionFreeHeader;
      final int freeHeader = (int) (lastPositionFreeHeader >>> 32);

      if (position == freeHeader) {
        assert position == 0;

        return Optional.empty();
      }

      return Optional.of(new OLogSequenceNumber(segmentIndex, position));
    } finally {
      syncLock.sharedUnlock();
    }
  }

  @Override
  public Optional<Long> segmentIndex() {
    return Optional.of(segmentIndex);
  }

  @Override
  public void close() throws Exception {
    syncLock.exclusiveLock();
    try {
      doClose();
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void doClose() {
    if (closed) {
      return;
    }

    doSync();

    IoUtil.unmap(buffer);

    closed = true;
  }

  public void delete() throws IOException {
    syncLock.exclusiveUnlock();
    try {
      checkForClose();

      doClose();

      Files.delete(segmentPath);
    } finally {
      syncLock.exclusiveUnlock();
    }
  }

  private void checkForClose() {
    if (closed) {
      throw new IllegalStateException(
          "Segment with index " + segmentIndex + " is closed and can not be used");
    }
  }
}
