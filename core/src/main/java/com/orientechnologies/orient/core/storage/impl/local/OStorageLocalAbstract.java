/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.serialization.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OStorageLocalAbstract extends OStorageEmbedded implements OFreezableStorage {
  protected OWriteAheadLog      writeAheadLog;
  protected OStorageTransaction transaction = null;
  protected ODiskCache          diskCache;

  public OStorageLocalAbstract(String name, String filePath, String mode) {
    super(name, filePath, mode);
  }

  public abstract OStorageVariableParser getVariableParser();

  public abstract String getMode();

  public abstract String getStoragePath();

  protected abstract OPhysicalPosition updateRecord(OCluster cluster, ORecordId rid, byte[] recordContent,
      ORecordVersion recordVersion, byte recordType);

  protected abstract OPhysicalPosition createRecord(ODataLocal dataSegment, OCluster cluster, byte[] recordContent,
      byte recordType, ORecordId rid, ORecordVersion recordVersion);

  public abstract ODiskCache getDiskCache();

  public abstract boolean wasClusterSoftlyClosed(String clusterIndexName);

  public abstract boolean check(boolean b, OCommandOutputListener dbCheckTest);

  public OStorageTransaction getStorageTransaction() {
    lock.acquireSharedLock();
    try {
      return transaction;
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, final Callable<Object> callable) throws IOException {
    freeze(false);
    try {
      if (callable != null)
        try {
          callable.call();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on callback invocation during backup", e);
        }

      OZIPCompressionUtil.compressDirectory(getStoragePath(), out);
    } finally {
      release();
    }
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, final Callable<Object> callable) throws IOException {
    if (!isClosed())
      close();

    OZIPCompressionUtil.uncompressDirectory(in, getStoragePath());
  }

  protected void endStorageTx() throws IOException {
    if (writeAheadLog == null)
      return;

    writeAheadLog.log(new OAtomicUnitEndRecord(transaction.getOperationUnitId(), false));
  }

  protected void startStorageTx(OTransaction clientTx) throws IOException {
    if (writeAheadLog == null)
      return;

    if (transaction != null && transaction.getClientTx().getId() != clientTx.getId())
      rollback(clientTx);

    transaction = new OStorageTransaction(clientTx, OOperationUnitId.generateId());

    OLogSequenceNumber startLSN = writeAheadLog.log(new OAtomicUnitStartRecord(true, transaction.getOperationUnitId()));
    transaction.setStartLSN(startLSN);
  }

  protected void rollbackStorageTx() throws IOException {
    if (writeAheadLog == null || transaction == null)
      return;

    writeAheadLog.log(new OAtomicUnitEndRecord(transaction.getOperationUnitId(), true));
    final List<OWALRecord> operationUnit = readOperationUnit(transaction.getStartLSN(), transaction.getOperationUnitId());
    undoOperation(operationUnit);
  }

  private List<OWALRecord> readOperationUnit(OLogSequenceNumber startLSN, OOperationUnitId unitId) throws IOException {
    final OLogSequenceNumber beginSequence = writeAheadLog.begin();

    if (startLSN == null)
      startLSN = beginSequence;

    if (startLSN.compareTo(beginSequence) < 0)
      startLSN = beginSequence;

    List<OWALRecord> operationUnit = new ArrayList<OWALRecord>();

    OLogSequenceNumber lsn = startLSN;
    while (lsn != null) {
      OWALRecord record = writeAheadLog.read(lsn);
      if (!(record instanceof OOperationUnitRecord)) {
        lsn = writeAheadLog.next(lsn);
        continue;
      }

      OOperationUnitRecord operationUnitRecord = (OOperationUnitRecord) record;
      if (operationUnitRecord.getOperationUnitId().equals(unitId)) {
        operationUnit.add(record);
        if (record instanceof OAtomicUnitEndRecord)
          break;
      }
      lsn = writeAheadLog.next(lsn);
    }

    return operationUnit;
  }

  protected void undoOperation(List<OWALRecord> operationUnit) throws IOException {
    for (int i = operationUnit.size() - 1; i >= 0; i--) {
      OWALRecord record = operationUnit.get(i);
      if (checkFirstAtomicUnitRecord(i, record)) {
        assert ((OAtomicUnitStartRecord) record).isRollbackSupported();
        continue;
      }

      if (checkLastAtomicUnitRecord(i, record, operationUnit.size())) {
        assert ((OAtomicUnitEndRecord) record).isRollback();
        continue;
      }

      if (record instanceof OUpdatePageRecord) {
        OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) record;
        final long fileId = updatePageRecord.getFileId();
        final long pageIndex = updatePageRecord.getPageIndex();

        if (!diskCache.isOpen(fileId))
          diskCache.openFile(fileId);

        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, true);
        OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.acquireExclusiveLock();
        try {
          ODurablePage durablePage = new ODurablePage(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);

          OPageChanges pageChanges = updatePageRecord.getChanges();
          durablePage.revertChanges(pageChanges);

          durablePage.setLsn(updatePageRecord.getPrevLsn());
        } finally {
          cachePointer.releaseExclusiveLock();
          diskCache.release(cacheEntry);
        }
      } else {
        OLogManager.instance().error(this, "Invalid WAL record type was passed %s. Given record will be skipped.",
            record.getClass());
        assert false : "Invalid WAL record type was passed " + record.getClass().getName();
      }
    }
  }

  protected boolean checkFirstAtomicUnitRecord(int index, OWALRecord record) {
    boolean isAtomicUnitStartRecord = record instanceof OAtomicUnitStartRecord;
    if (isAtomicUnitStartRecord && index != 0) {
      OLogManager.instance().error(this, "Record %s should be the first record in WAL record list.",
          OAtomicUnitStartRecord.class.getName());
      assert false : "Record " + OAtomicUnitStartRecord.class.getName() + " should be the first record in WAL record list.";
    }

    if (index == 0 && !isAtomicUnitStartRecord) {
      OLogManager.instance().error(this, "Record %s should be the first record in WAL record list.",
          OAtomicUnitStartRecord.class.getName());
      assert false : "Record " + OAtomicUnitStartRecord.class.getName() + " should be the first record in WAL record list.";
    }

    return isAtomicUnitStartRecord;
  }

  protected boolean checkLastAtomicUnitRecord(int index, OWALRecord record, int size) {
    boolean isAtomicUnitEndRecord = record instanceof OAtomicUnitEndRecord;
    if (isAtomicUnitEndRecord && index != size - 1) {
      OLogManager.instance().error(this, "Record %s should be the last record in WAL record list.",
          OAtomicUnitEndRecord.class.getName());
      assert false : "Record " + OAtomicUnitEndRecord.class.getName() + " should be the last record in WAL record list.";
    }

    if (index == size - 1 && !isAtomicUnitEndRecord) {
      OLogManager.instance().error(this, "Record %s should be the last record in WAL record list.",
          OAtomicUnitEndRecord.class.getName());
      assert false : "Record " + OAtomicUnitEndRecord.class.getName() + " should be the last record in WAL record list.";
    }

    return isAtomicUnitEndRecord;
  }

  public OWriteAheadLog getWALInstance() {
    lock.acquireSharedLock();
    try {
      return writeAheadLog;
    } finally {
      lock.releaseSharedLock();
    }
  }

}
