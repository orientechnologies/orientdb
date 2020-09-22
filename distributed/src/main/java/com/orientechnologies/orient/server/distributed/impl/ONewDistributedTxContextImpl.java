package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.concur.lock.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import java.io.IOException;
import java.util.*;

public class ONewDistributedTxContextImpl implements ODistributedTxContext {

  // This will be replaced once we have versioned keys.
  public static final int DEFAULT_INDEX_KEY_VER = 0;

  public enum Status {
    FAILED,
    SUCCESS,
    TIMEDOUT,
  }

  private final ODistributedDatabaseImpl shared;
  private final ODistributedRequestId id;
  private final OTransactionInternal tx;
  private final long startedOn;
  private final Set<OTxPromise<ORID>> promisedRids = new HashSet<>();
  private final Set<OTxPromise<Object>> promisedKeys = new HashSet<>();

  private Status status;
  private final OTransactionId transactionId;

  public ONewDistributedTxContextImpl(
      ODistributedDatabaseImpl shared,
      ODistributedRequestId reqId,
      OTransactionInternal tx,
      OTransactionId id) {
    this.shared = shared;
    this.id = reqId;
    this.tx = tx;
    this.startedOn = System.currentTimeMillis();
    transactionId = id;
  }

  @Override
  public OTransactionId acquireIndexKeyPromise(Object key, int version, boolean force) {
    OTxPromiseManager<Object> promiseManager = shared.getIndexKeyPromiseManager();
    OTransactionId cancelledPromise = null;
    try {
      cancelledPromise = promiseManager.promise(key, version, transactionId, force);
    } catch (OTxPromiseException ex) {
      this.releasePromises();
      throw new ODistributedKeyLockedException(
          shared.getLocalNodeName(), key, promiseManager.getTimeout());
    }
    promisedKeys.add(new OTxPromise<>(key, version, transactionId));
    return cancelledPromise;
  }

  @Override
  public OTransactionId acquirePromise(ORID rid, int version, boolean force) {
    OTxPromiseManager<ORID> promiseManager = shared.getRecordPromiseManager();
    OTransactionId cancelledPromise = null;
    try {
      cancelledPromise = promiseManager.promise(rid, version, transactionId, force);
    } catch (OTxPromiseException ex) {
      // todo(PS): should we distinguish between timeout and version mismatch when force=true?
      this.releasePromises();
      throw new ODistributedRecordLockedException(
          shared.getLocalNodeName(), rid, promiseManager.getTimeout());
    }
    promisedRids.add(new OTxPromise<>(rid, version, transactionId));
    return cancelledPromise;
  }

  @Override
  public ODistributedRequestId getReqId() {
    return id;
  }

  @Override
  public synchronized void begin(ODatabaseDocumentInternal database, boolean local) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void commit(ODatabaseDocumentInternal database) {
    ODistributedDatabase localDistributedDatabase =
        ((ODatabaseDocumentDistributed) database).getDistributedShared();
    OTxMetadataHolder metadataHolder = localDistributedDatabase.commit(getTransactionId());
    try {
      tx.setMetadataHolder(Optional.of(metadataHolder));
      tx.prepareSerializedOperations();
      ((ODatabaseDocumentDistributed) database).internalCommit2pc(this);
    } catch (IOException e) {
      throw OException.wrapException(
          new ODistributedException("Error on preparation of log serialized operations"), e);
    } finally {
      metadataHolder.notifyMetadataRead();
    }
  }

  @Override
  public Set<ORecordId> rollback(ODatabaseDocumentInternal database) {
    return new HashSet<>();
  }

  @Override
  public void destroy() {
    releasePromises();
  }

  @Override
  public void clearUndo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releasePromises() {
    shared.rollback(this.transactionId);
    for (OTxPromise<ORID> promise : promisedRids) {
      shared.getRecordPromiseManager().release(promise.getKey(), promise.getVersion(), transactionId);
    }
    promisedRids.clear();
    for (Object promisedKey : promisedKeys) {
      shared.getIndexKeyPromiseManager().release(promisedKey, DEFAULT_INDEX_KEY_VER, transactionId);
    }
    promisedKeys.clear();
  }

  @Override
  public long getStartedOn() {
    return startedOn;
  }

  @Override
  public Set<ORecordId> cancel(
      ODistributedServerManager current, ODatabaseDocumentInternal database) {
    destroy();
    return null;
  }

  @Override
  public OTransactionInternal getTransaction() {
    return tx;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public Set<OTxPromise<ORID>> getPromisedRids() {
    return promisedRids;
  }

  public Set<OTxPromise<Object>> getPromisedKeys() {
    return promisedKeys;
  }

  @Override
  public OTransactionId getTransactionId() {
    return transactionId;
  }
}
