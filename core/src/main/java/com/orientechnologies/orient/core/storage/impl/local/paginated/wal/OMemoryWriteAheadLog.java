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

import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/25/14
 */
public class OMemoryWriteAheadLog extends OAbstractWriteAheadLog {
  @Override
  public OLogSequenceNumber begin() throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber end() {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public void flush() {
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) throws IOException {
    return log(new OAtomicUnitStartRecord(isRollbackSupported, unitId));
  }

  @Override
  public OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback,
      OLogSequenceNumber startLsn, Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) throws IOException {
    return log(new OAtomicUnitEndRecord(operationUnitId, rollback, atomicOperationMetadata));
  }

  @Override
  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    return new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void truncate() throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void close(boolean flush) throws IOException {
  }

  @Override
  public void delete() throws IOException {
  }

  @Override
  public void delete(boolean flush) throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber getFlushedLsn() {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public void cutTill(OLogSequenceNumber lsn) throws IOException {
  }

  @Override
  public void addFullCheckpointListener(OCheckpointRequestListener listener) {
  }

  @Override
  public void removeFullCheckpointListener(OCheckpointRequestListener listener) {
  }

  @Override
  public void moveLsnAfter(OLogSequenceNumber lsn) {
  }

  @Override
  public void preventCutTill(OLogSequenceNumber lsn) throws IOException {
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

  @Override
  public void newSegment() throws IOException {
  }

  @Override
  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public OLogSequenceNumber begin(long segmentId) throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public void cutAllSegmentsSmallerThan(long segmentId) throws IOException {
  }
}
