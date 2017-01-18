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
public interface OWriteAheadLog {
  OLogSequenceNumber logFuzzyCheckPointStart(OLogSequenceNumber flushedLsn) throws IOException;

  OLogSequenceNumber logFuzzyCheckPointEnd() throws IOException;

  OLogSequenceNumber logFullCheckpointStart() throws IOException;

  OLogSequenceNumber logFullCheckpointEnd() throws IOException;

  OLogSequenceNumber getLastCheckpoint();

  OLogSequenceNumber begin() throws IOException;

  OLogSequenceNumber begin(long segmentId) throws IOException;

  OLogSequenceNumber end();

  void flush() throws IOException;

  OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) throws IOException;

  OLogSequenceNumber logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback, OLogSequenceNumber startLsn,
      Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadata) throws IOException;

  OLogSequenceNumber log(OWALRecord record) throws IOException;

  void truncate() throws IOException;

  void close() throws IOException;

  void close(boolean flush) throws IOException;

  void delete() throws IOException;

  void delete(boolean flush) throws IOException;

  OWALRecord read(OLogSequenceNumber lsn) throws IOException;

  OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException;

  OLogSequenceNumber getFlushedLsn();

  void cutTill(OLogSequenceNumber lsn) throws IOException;

  void cutAllSegmentsSmallerThan(long segmentId) throws IOException;

  void addFullCheckpointListener(OCheckpointRequestListener listener);

  void removeFullCheckpointListener(OCheckpointRequestListener listener);

  void addLowDiskSpaceListener(OLowDiskSpaceListener listener);

  void removeLowDiskSpaceListener(OLowDiskSpaceListener listener);

  void moveLsnAfter(OLogSequenceNumber lsn) throws IOException;

  void preventCutTill(OLogSequenceNumber lsn) throws IOException;

  File[] nonActiveSegments(long fromSegment);

  long[] nonActiveSegments();

  long activeSegment();

  void newSegment() throws IOException;
}
