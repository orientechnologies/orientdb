/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public abstract class OStorageLocalAbstract extends OStorageEmbedded implements OFreezableStorage {
  protected volatile OWriteAheadLog                writeAheadLog;
  protected volatile ODiskCache                    diskCache;
  protected volatile OAtomicOperationsManager      atomicOperationsManager;

  protected final ThreadLocal<OStorageTransaction> transaction = new ThreadLocal<OStorageTransaction>();

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
    return transaction.get();
  }

  public OAtomicOperationsManager getAtomicOperationsManager() {
    return atomicOperationsManager;
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
    atomicOperationsManager.endAtomicOperation(false);

    assert atomicOperationsManager.getCurrentOperation() == null;
  }

  protected void startStorageTx(OTransaction clientTx) throws IOException {
    if (writeAheadLog == null)
      return;

    final OStorageTransaction storageTx = transaction.get();
    if (storageTx != null && storageTx.getClientTx().getId() != clientTx.getId())
      rollback(clientTx);

    atomicOperationsManager.startAtomicOperation();
    transaction.set(new OStorageTransaction(clientTx));
  }

  protected void rollbackStorageTx() throws IOException {
    if (writeAheadLog == null || transaction.get() == null)
      return;

    final OAtomicOperation operation = atomicOperationsManager.endAtomicOperation(true);

    assert atomicOperationsManager.getCurrentOperation() == null;

    final List<OWALRecord> operationUnit = readOperationUnit(operation.getStartLSN(), operation.getOperationUnitId());
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

          durablePage.setLsn(updatePageRecord.getLsn());
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
    return writeAheadLog;
  }

}
