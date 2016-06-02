package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.List;

public interface ODistributedTxContext {
  void lock(final ORID rid);

  void addUndoTask(final ORemoteTask undoTask);

  ODistributedRequestId getReqId();

  void commit();

  void fix(ODatabaseDocumentInternal database, List<ORemoteTask> fixTasks);

  int rollback(final ODatabaseDocumentInternal database);

  void destroy();

  void unlock();
}