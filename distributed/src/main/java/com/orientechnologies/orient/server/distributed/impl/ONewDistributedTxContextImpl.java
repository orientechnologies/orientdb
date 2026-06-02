package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.exception.OTxPromiseException;
import com.orientechnologies.orient.server.distributed.impl.lock.OTxPromise;
import com.orientechnologies.orient.server.distributed.impl.lock.OTxPromiseManager;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ONewDistributedTxContextImpl implements ODistributedTxContext {

  private final OSharedContextDistributed shared;
  private final ODistributedRequestId id;
  private final OTransactionInternal tx;
  private final long startedOn;
  private final Set<OTxPromise<ORID>> promisedRids = new HashSet<>();
  private final Set<OTxPromise<Object>> promisedKeys = new HashSet<>();
  private final OTransactionIdPromise promise;
  private TxContextStatus status;

  public ONewDistributedTxContextImpl(
      OSharedContextDistributed shared,
      ODistributedRequestId reqId,
      OTransactionInternal tx,
      OTransactionIdPromise id) {
    this.shared = shared;
    this.id = reqId;
    this.tx = tx;
    this.startedOn = System.currentTimeMillis();
    promise = id;
  }

  @Override
  public OTransactionId acquireIndexKeyPromise(Object key, int version, boolean force) {
    var distributeContext = shared.getDistributedContext();
    OTxPromiseManager<Object> promiseManager = distributeContext.getIndexKeyPromiseManager();
    OTransactionId cancelledPromise = null;
    try {
      cancelledPromise = promiseManager.promise(key, version, promise.getId(), force);
    } catch (OTxPromiseException ex) {
      this.releasePromises();
      throw new ODistributedKeyLockedException(distributeContext.getLocalNodeName(), key);
    }
    promisedKeys.add(new OTxPromise<>(key, version, promise.getId()));
    return cancelledPromise;
  }

  @Override
  public OTransactionId acquirePromise(ORID rid, int version, boolean force) {
    var distributeContext = shared.getDistributedContext();
    OTxPromiseManager<ORID> promiseManager = distributeContext.getRecordPromiseManager();
    OTransactionId cancelledPromise = null;
    try {
      cancelledPromise = promiseManager.promise(rid, version, promise.getId(), force);
    } catch (OTxPromiseException ex) {
      this.releasePromises();
      throw new ODistributedRecordLockedException(distributeContext.getLocalNodeName(), rid);
    }
    promisedRids.add(new OTxPromise<>(rid, version, promise.getId()));
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
    OSharedContextDistributed context =
        ((ODatabaseDocumentDistributed) database).getSharedContext();
    OTxMetadataHolder metadataHolder = context.getTransactionSequence().notifySuccess(promise);
    try {
      tx.setMetadataHolder(metadataHolder);
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
  public void destroy() {
    releasePromises();
  }

  @Override
  public void releasePromises() {
    shared.getTransactionSequence().notifyFailure(this.promise);
    var recordPromiseManager = shared.getDistributedContext().getRecordPromiseManager();
    for (OTxPromise<ORID> promise : promisedRids) {
      recordPromiseManager.release(promise.getKey(), this.promise.getId());
    }
    promisedRids.clear();
    var indexPromiseManager = shared.getDistributedContext().getIndexKeyPromiseManager();
    for (OTxPromise<Object> promisedKey : promisedKeys) {
      indexPromiseManager.release(promisedKey.getKey(), this.promise.getId());
    }
    promisedKeys.clear();
  }

  @Override
  public long getStartedOn() {
    return startedOn;
  }

  @Override
  public void cancel() {
    destroy();
  }

  @Override
  public OTransactionInternal getTransaction() {
    return tx;
  }

  public TxContextStatus getStatus() {
    return status;
  }

  public void setStatus(TxContextStatus status) {
    this.status = status;
  }

  public Set<OTxPromise<ORID>> getPromisedRids() {
    return promisedRids;
  }

  public Set<OTxPromise<Object>> getPromisedKeys() {
    return promisedKeys;
  }

  @Override
  public OTransactionId getTransactionId() {
    return promise.getId();
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }
}
