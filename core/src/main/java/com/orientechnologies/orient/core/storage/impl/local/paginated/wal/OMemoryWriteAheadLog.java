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
  private long             counter = 0;
  private List<OWALRecord> records = new ArrayList<OWALRecord>();

  @Override
  public OLogSequenceNumber begin() throws IOException {
    syncObject.lock();
    try {
      if (records.isEmpty())
        return null;

      return records.get(0).getLsn();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber end() throws IOException {
    syncObject.lock();
    try {
      if (records.isEmpty())
        return null;

      return records.get(records.size() - 1).getLsn();
    } finally {
      syncObject.unlock();
    }
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
    OLogSequenceNumber logSequenceNumber;
    syncObject.lock();
    try {
      logSequenceNumber = new OLogSequenceNumber(0, counter);
      counter++;

      if (record instanceof OAtomicUnitStartRecord)
        records.clear();

      records.add(record);
      record.setLsn(logSequenceNumber);
    } finally {
      syncObject.unlock();
    }

    return logSequenceNumber;
  }

  @Override
  public void truncate() throws IOException {
    syncObject.lock();
    try {
      records.clear();
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void close(boolean flush) throws IOException {
  }

  @Override
  public void delete() throws IOException {
    truncate();
  }

  @Override
  public void delete(boolean flush) throws IOException {
    truncate();
  }

  @Override
  public OWALRecord read(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      if (records.isEmpty())
        return null;

      final long index = lsn.getPosition() - records.get(0).getLsn().getPosition();
      if (index < 0 || index >= records.size())
        return null;

      return records.get((int) index);
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber next(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      if (records.isEmpty())
        return null;

      final long index = lsn.getPosition() - records.get(0).getLsn().getPosition() + 1;
      if (index < 0 || index >= records.size())
        return null;

      return new OLogSequenceNumber(0, lsn.getPosition() + 1);
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public OLogSequenceNumber getFlushedLSN() {
    return new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void cutTill(OLogSequenceNumber lsn) throws IOException {
    syncObject.lock();
    try {
      if (records.isEmpty())
        return;

      long index = records.get(0).getLsn().getPosition() - lsn.getPosition();
      if (index < 0)
        return;

      if (index > records.size())
        index = records.size();

      for (int i = 0; i < index; i++)
        records.remove(0);
    } finally {
      syncObject.unlock();
    }
  }

  @Override
  public void addFullCheckpointListener(OFullCheckpointRequestListener listener) {
  }

  @Override
  public void removeFullCheckpointListener(OFullCheckpointRequestListener listener) {
  }
}
