package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OPersistentOperationalLogV1;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralMaster;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralSlave;

import java.util.concurrent.Executors;

public class OStructuralDistributedContext {
  private OStructuralSubmitContext submitContext;
  private OOperationLog            opLog;
  private OrientDBDistributed      context;
  private OStructuralMaster        master;
  private OStructuralSlave         slave;

  public OStructuralDistributedContext(OrientDBDistributed context) {
    this.context = context;
    initOpLog();
    submitContext = new OStructuralSubmitContextImpl();
    slave = new OStructuralSlave(Executors.newSingleThreadExecutor(), opLog, context);
    master = null;
  }

  private void initOpLog() {
    this.opLog = OPersistentOperationalLogV1
        .newInstance("OSystem", context, (x) -> context.getCoordinateMessagesFactory().createStructuralOperationRequest(x));
  }

  public OStructuralSubmitContext getSubmitContext() {
    return submitContext;
  }

  public OOperationLog getOpLog() {
    return opLog;
  }

  public OStructuralMaster getMaster() {
    return master;
  }

  public OStructuralSlave getSlave() {
    return slave;
  }

  public synchronized void makeMaster(ONodeIdentity identity, OStructuralSharedConfiguration sharedConfiguration) {
    if (master == null) {
      int quorum = sharedConfiguration.getQuorum();
      int timeout = 100;
      master = new OStructuralMaster(Executors.newSingleThreadExecutor(), opLog, context, quorum, timeout);
    }
  }

  public synchronized void setExternalMaster(OStructuralDistributedMember coordinator) {
    if (this.master != null) {
      this.master.close();
      this.master = null;
    }
    this.getSubmitContext().setMaster(coordinator);
  }

  public synchronized void close() {
    if (master != null)
      master.close();
    slave.close();
  }

  public void execute(ONodeIdentity senderNode, OSessionOperationId operationId, OStructuralSubmitRequest request) {
    if (master != null) {
      master.receiveSubmit(senderNode, operationId, request);
    }
  }
}
