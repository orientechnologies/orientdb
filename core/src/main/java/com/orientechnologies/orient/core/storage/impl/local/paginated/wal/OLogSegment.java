package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Basic interface for classes which present log segments of WAL.
 * Main reason of creation of this interface is support of different binary formats
 * of WAL in the same deployment.
 * <p>
 * To detect which version of binary format is stored we use value stored under
 * {@link OWALPage#MAGIC_NUMBER_OFFSET}
 */
public interface OLogSegment extends Comparable<OLogSegment> {
  long getOrder();

  void init(ByteBuffer buffer) throws IOException;

  long filledUpTo();

  OLogSequenceNumber begin() throws IOException;

  OLogSequenceNumber end();

  String getPath();

  OLogSequenceNumber logRecord(byte[] record);

  byte[] readRecord(OLogSequenceNumber lsn, ByteBuffer byteBuffer) throws IOException;

  OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn, ByteBuffer buffer) throws IOException;

  void close(boolean flush) throws IOException;

  void flush();

  void delete(boolean flush) throws IOException;

  void stopFlush(boolean flush);

  void startFlush();
}
