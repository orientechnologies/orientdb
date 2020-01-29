package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

public class OWaitPartitionsReadyTask extends OAbstractRemoteTask {
  private volatile boolean      hasResponse = false;
  private final    OExecuteOnce execute;

  public OWaitPartitionsReadyTask(OExecuteOnce execute) {
    this.execute = execute;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return null;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    if (execute.execute(requestId, iServer, iManager, database)) {
      this.hasResponse = execute.getToRun().hasResponse();
      return execute.getResult();
    }
    return null;
  }

  @Override
  public boolean hasResponse() {
    return hasResponse;
  }

  @Override
  public String toString() {
    return "Blocking thread for: " + execute.getToRun().toString();
  }

  @Override
  public int getFactoryId() {
    return 0;
  }

  @Override
  public boolean isUsingDatabase() {
    return execute.getToRun().isUsingDatabase();
  }

  public ORemoteTask getInternal() {
    return execute.getToRun();
  }
}
