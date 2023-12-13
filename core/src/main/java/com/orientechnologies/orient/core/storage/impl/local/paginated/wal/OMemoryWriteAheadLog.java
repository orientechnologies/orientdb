/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/25/14
 */
public class OMemoryWriteAheadLog extends OAbstractWriteAheadLog {
  private final AtomicInteger nextPosition = new AtomicInteger();
  private final AtomicInteger nextOperationId = new AtomicInteger();

  private volatile ByteArrayOutputStream buffer;
  private final String storageName;

  public OMemoryWriteAheadLog(String storageName) {
    this.storageName = storageName;
  }

  private void startBuffering() {
    buffer = new ByteArrayOutputStream();
  }

  private void stopBuffering() {
    buffer = null;
  }

  @Override
  public OLogSequenceNumber begin() {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber end() {
    return new OLogSequenceNumber(-1, -1);
  }

  @Override
  public void flush() {}

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(
      boolean isRollbackSupported, long unitId) {
    return log(new OAtomicUnitStartRecord(isRollbackSupported, unitId));
  }

  public OLogSequenceNumber logAtomicOperationStartRecord(
      final boolean isRollbackSupported, final long unitId, byte[] metadata) {
    final OAtomicUnitStartMetadataRecord record =
        new OAtomicUnitStartMetadataRecord(isRollbackSupported, unitId, metadata);
    return log(record);
  }

  @Override
  public OLogSequenceNumber logAtomicOperationEndRecord(
      long operationUnitId,
      boolean rollback,
      OLogSequenceNumber startLsn,
      Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) {
    return log(new OAtomicUnitEndRecord(operationUnitId, rollback, atomicOperationMetadata));
  }

  @Override
  public OLogSequenceNumber log(WriteableWALRecord record) {
    final OLogSequenceNumber lsn = new OLogSequenceNumber(0, nextPosition.incrementAndGet());
    record.setLsn(lsn);
    OutputStream out = this.buffer;
    if (out != null) {
      ByteBuffer serializedRecord = OWALRecordsFactory.toStream(record);
      try {
        byte[] recordSize = new byte[OIntegerSerializer.INT_SIZE];
        OIntegerSerializer.INSTANCE.serializeNative(serializedRecord.limit(), recordSize, 0);
        out.write(recordSize);
        out.write(serializedRecord.array());
      } catch (IOException e) {
        throw OException.wrapException(new OStorageException(e.getMessage()), e);
      }
    }

    return lsn;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void close(boolean flush) throws IOException {}

  @Override
  public void delete() throws IOException {}

  @Override
  public List<WriteableWALRecord> read(OLogSequenceNumber lsn, int limit) throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public List<WriteableWALRecord> next(OLogSequenceNumber lsn, int limit) {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber getFlushedLsn() {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public boolean cutTill(OLogSequenceNumber lsn) {
    return false;
  }

  @Override
  public void addCheckpointListener(OCheckpointRequestListener listener) {}

  @Override
  public void removeCheckpointListener(OCheckpointRequestListener listener) {}

  @Override
  public void moveLsnAfter(OLogSequenceNumber lsn) {}

  @Override
  public void addCutTillLimit(OLogSequenceNumber lsn) {
    startBuffering();
  }

  @Override
  public void removeCutTillLimit(OLogSequenceNumber lsn) {
    stopBuffering();
  }

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

  public void backupBuffer(ZipOutputStream output) throws IOException {
    final ZipEntry entry = new ZipEntry(getSegmentName(0));
    output.putNextEntry(entry);
    ByteArrayOutputStream out = this.buffer;
    this.buffer = null;
    output.write(out.toByteArray());
  }

  private String getSegmentName(final long segment) {
    return storageName + "." + segment + WAL_SEGMENT_EXTENSION;
  }

  @Override
  public OLogSequenceNumber begin(long segmentId) {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public boolean cutAllSegmentsSmallerThan(long segmentId) {
    return false;
  }

  @Override
  public void addEventAt(OLogSequenceNumber lsn, Runnable event) {
    event.run();
  }

  @Override
  public boolean appendNewSegment() {
    return false;
  }
}
