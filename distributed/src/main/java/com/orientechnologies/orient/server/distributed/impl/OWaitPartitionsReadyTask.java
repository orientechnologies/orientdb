package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Function;

public class OWaitPartitionsReadyTask extends OAbstractRemoteTask {
  private final    CyclicBarrier started;
  private final    ORemoteTask   toRun;
  private final    CyclicBarrier finished;
  private volatile boolean       hasResponse = false;

  public OWaitPartitionsReadyTask(CyclicBarrier started, ORemoteTask toRun, CyclicBarrier finished) {
    this.started = started;
    this.toRun = toRun;
    this.finished = finished;
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
    Object result = null;
    if (started.await() == 0) {
      result = toRun.execute(requestId, iServer, iManager, database);
      hasResponse = toRun.hasResponse();
    }
    finished.await();

    return result;
  }

  @Override
  public boolean hasResponse() {
    return hasResponse;
  }

  @Override
  public String toString() {
    return "Barrier on: " + toRun.toString();
  }

  @Override
  public int getFactoryId() {
    return 0;
  }

  @Override
  public boolean isUsingDatabase() {
    return toRun.isUsingDatabase();
  }

}
