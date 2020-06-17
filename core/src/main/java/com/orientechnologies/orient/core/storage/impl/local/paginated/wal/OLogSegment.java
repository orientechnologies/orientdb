package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Basic interface for classes which present log segments of WAL.
 *
 * <p>WAL is split by segments. Segments are used to truncate WAL by portions. All transactions
 * which are started inside of segment should not cross segment. So they should be finished before
 * new segment will be started.
 *
 * <p>Main reason of creation of this interface is support of different binary formats of WAL in the
 * same deployment.
 *
 * <p>To detect which version of binary format is stored we use value stored under {@link
 * OWALPage#MAGIC_NUMBER_OFFSET}
 */
public interface OLogSegment extends Comparable<OLogSegment> {
  /** @return index if segment inside of WAL. */
  long getOrder();

  /**
   * This method should be called before segment started to be used by WAL. It performs
   * initialisation of internal state of segment.
   */
  void init() throws IOException;

  /** @return length of segment in pages. */
  long filledUpTo();

  /** @return LSN of first record contained inside of segment. */
  OLogSequenceNumber begin() throws IOException;

  /** @return LSN of last record contained inside of segment. */
  OLogSequenceNumber end();

  /** @return Location of the buffer inside the file system. */
  Path getPath();

  /** Appends new records to the WAL segment and returns LSN of it. */
  OLogSequenceNumber logRecord(byte[] record);

  /** Reads WAL record from segment from position indicated by LSN. */
  byte[] readRecord(OLogSequenceNumber lsn) throws IOException;

  /**
   * Returns LSN of the record which follows after the record with passed in LSN. If passed in LSN
   * belongs to the last record <code>null</code> is returned.
   */
  OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException;

  /**
   * Writes buffer of the segment to the disk and closes all acquired resources. Performs <code>
   * fsync</code> of data if necessary.
   */
  void close(boolean flush) throws IOException;

  /** Writes buffer of the segment to the disk and performs <code>fsync</code> of data. */
  void flush();

  /** Clears the buffer and deletes file content. */
  void delete(boolean flush) throws IOException;

  /**
   * Stop background task which writes data from log segments buffer to the disk and writes the rest
   * of the buffer to the disk. Performs <code>fsync</code> if necessary.
   */
  void stopBackgroundWrite(boolean flush);

  /** Start background task which performs periodical write of background buffer to the disk. */
  void startBackgroundWrite();
}
