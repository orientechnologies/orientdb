/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.OFullCheckpointRequestListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 6/25/14
 */
public class OMemoryWriteAheadLog extends OAbstractWriteAheadLog {
  @Override
  public OLogSequenceNumber begin() throws IOException {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public OLogSequenceNumber end() throws IOException {
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
      OLogSequenceNumber startLsn) throws IOException {
    return log(new OAtomicUnitEndRecord(operationUnitId, rollback, startLsn));
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
  public OLogSequenceNumber getFlushedLSN() {
    throw new UnsupportedOperationException("Operation not supported for in memory storage.");
  }

  @Override
  public void cutTill(OLogSequenceNumber lsn) throws IOException {
  }

  @Override
  public void addFullCheckpointListener(OFullCheckpointRequestListener listener) {
  }

  @Override
  public void removeFullCheckpointListener(OFullCheckpointRequestListener listener) {
  }
}
