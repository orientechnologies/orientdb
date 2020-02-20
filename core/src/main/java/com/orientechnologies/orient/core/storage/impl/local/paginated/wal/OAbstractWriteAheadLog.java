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

import com.orientechnologies.orient.core.exception.OStorageException;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/25/14
 */
public abstract class OAbstractWriteAheadLog implements OWriteAheadLog {
  private boolean closed;

  private final Lock               syncObject = new ReentrantLock();
  private       OLogSequenceNumber lastCheckpoint;

  @Override
  public OLogSequenceNumber logFuzzyCheckPointStart(OLogSequenceNumber flushedLsn, Optional<byte[]> lastMetadata) throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      OFuzzyCheckpointStartRecord record = new OFuzzyCheckpointStartRecord(lastCheckpoint, flushedLsn);
      log(record);
      return record.getLsn();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber logFuzzyCheckPointEnd() throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      OFuzzyCheckpointEndRecord record = new OFuzzyCheckpointEndRecord();
      log(record);
      return record.getLsn();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber logFullCheckpointStart(Optional<byte[]> lastMetadata) throws IOException {
    return log(new OFullCheckpointStartRecord(lastCheckpoint));
  }

  @Override
  public OLogSequenceNumber logFullCheckpointEnd() throws IOException {
    syncObject.lock();
    try {
      checkForClose();

      return log(new OCheckpointEndRecord());
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber getLastCheckpoint() {
    syncObject.lock();
    try {
      checkForClose();

      return lastCheckpoint;
    } finally {
      syncObject.unlock();
    }
  }

  private void checkForClose() {
    if (closed)
      throw new OStorageException("WAL has been closed");
  }
}
