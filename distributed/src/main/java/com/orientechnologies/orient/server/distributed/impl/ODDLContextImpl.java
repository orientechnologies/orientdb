package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import java.util.Set;

public class ODDLContextImpl implements ODistributedTxContext {

  private String query;
  private OTransactionId preChangeId;
  private OTransactionId afterChangeId;
  private ODistributedRequestId requestId;
  private TxContextStatus status;

  public ODDLContextImpl(
      String query,
      OTransactionId preChangeId,
      OTransactionId afterChangeId,
      ODistributedRequestId requestId) {
    this.query = query;
    this.preChangeId = preChangeId;
    this.afterChangeId = afterChangeId;
    this.requestId = requestId;
  }

  @Override
  public ODistributedRequestId getReqId() {
    return requestId;
  }

  @Override
  public void commit(ODatabaseDocumentInternal database) {}

  @Override
  public Set<ORecordId> rollback(ODatabaseDocumentInternal database) {
    return null;
  }

  @Override
  public void destroy() {}

  @Override
  public void clearUndo() {}

  @Override
  public long getStartedOn() {
    return 0;
  }

  @Override
  public Set<ORecordId> cancel(
      ODistributedServerManager current, ODatabaseDocumentInternal database) {
    return null;
  }

  @Override
  public OTransactionInternal getTransaction() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OTransactionId getTransactionId() {
    return preChangeId;
  }

  @Override
  public void begin(ODatabaseDocumentInternal distributed, boolean local) {
    throw new UnsupportedOperationException();
  }

  public void setStatus(TxContextStatus status) {
    this.status = status;
  }

  public TxContextStatus getStatus() {
    return status;
  }

  public OTransactionId getPreChangeId() {
    return preChangeId;
  }

  public OTransactionId getAfterChangeId() {
    return afterChangeId;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public OTransactionId acquirePromise(ORID rid, int version, boolean force) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OTransactionId acquireIndexKeyPromise(Object key, int version, boolean force) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releasePromises() {
    throw new UnsupportedOperationException();
  }
}
