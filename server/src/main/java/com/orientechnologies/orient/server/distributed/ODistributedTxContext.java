package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

public interface ODistributedTxContext {
  void lock(final ORID rid);

  void addUndoTask(final ORemoteTask undoTask);

  ODistributedRequestId getReqId();

  void commit();

  void fix();

  int rollback(final ODatabaseDocumentTx database);
}