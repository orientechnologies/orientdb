package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.concurrent.CountDownLatch;

public class OExecuteOnce {
  private final    CountDownLatch start;
  private final    ORemoteTask    toRun;
  private volatile boolean        toExecute = true;
  private volatile Object         result;

  public OExecuteOnce(CountDownLatch start, ORemoteTask toRun) {
    this.start = start;
    this.toRun = toRun;
  }

  public boolean execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    start.countDown();
    start.await();
    synchronized (this) {
      if (toExecute) {
        toExecute = false;
        result = toRun.execute(requestId, iServer, iManager, database);
        return true;
      }
    }
    return false;
  }

  public Object getResult() {
    return result;
  }

  public ORemoteTask getToRun() {
    return toRun;
  }
}
