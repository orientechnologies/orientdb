package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.OPersistentOperationalLogV1;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;

import java.util.concurrent.Executors;

public class OStructuralDistributedContext {
  private OStructuralDistributedExecutor executor;
  private OStructuralSubmitContext       submitContext;
  private OStructuralCoordinator         coordinator;
  private OOperationLog                  opLog;
  private OrientDBDistributed            context;

  public OStructuralDistributedContext(OrientDBDistributed context) {
    this.context = context;
    initOpLog();
    executor = new OStructuralDistributedExecutor(Executors.newSingleThreadExecutor(), opLog, context);
    submitContext = new OStructuralSubmitContextImpl();
    coordinator = null;
  }

  private void initOpLog() {
    this.opLog = OPersistentOperationalLogV1
        .newInstance("OSystem", context, (x) -> context.getCoordinateMessagesFactory().createStructuralOperationRequest(x));
//    this.opLog = new OIncrementOperationalLog();
  }

  public OStructuralDistributedExecutor getExecutor() {
    return executor;
  }

  public OStructuralSubmitContext getSubmitContext() {
    return submitContext;
  }

  public synchronized OStructuralCoordinator getCoordinator() {
    return coordinator;
  }

  public OOperationLog getOpLog() {
    return opLog;
  }

  public synchronized void makeCoordinator(ONodeIdentity identity) {
    if (coordinator == null) {
      coordinator = new OStructuralCoordinator(Executors.newSingleThreadExecutor(), opLog, context);
      OStructuralLoopBackDistributeDistributedMember loopBack = new OStructuralLoopBackDistributeDistributedMember(identity,
          submitContext, coordinator, executor);
      coordinator.nodeConnected(loopBack);
      submitContext.setCoordinator(loopBack);
      executor.join(loopBack);
    }
  }

  public synchronized void setExternalCoordinator(OStructuralDistributedMember coordinator) {
    if (this.coordinator != null) {
      this.coordinator.close();
      this.coordinator = null;
    }
    submitContext.setCoordinator(coordinator);
    executor.join(coordinator);
  }

  public synchronized void close() {
    if (coordinator != null)
      coordinator.close();
    executor.close();
  }

}
