package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedLockManager;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedMember;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSharedConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class OStructuralMaster implements AutoCloseable, OMasterContext {
  private static final String                                           CONF_RESOURCE = "Configuration";
  private final        ExecutorService                                  executor;
  private final        OOperationLog                                    operationLog;
  private final        ConcurrentMap<OLogId, ORaftRequestContext>       contexts      = new ConcurrentHashMap<>();
  private final        Map<ONodeIdentity, OStructuralDistributedMember> members       = new ConcurrentHashMap<>();
  private final        Timer                                            timer;
  private final        OrientDBDistributed                              context;
  private              ODistributedLockManager                          lockManager   = new ODistributedLockManagerImpl();
  private              int                                              quorum;
  private              int                                              timeout;

  public OStructuralMaster(ExecutorService executor, OOperationLog operationLog, OrientDBDistributed context, int quorum,
      int timeout) {
    this.executor = executor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.context = context;
    this.quorum = quorum;
    this.timeout = timeout;
  }

  public void propagateAndApply(ORaftOperation operation, OpFinished finished) {
    executor.execute(() -> {
      OLogId id = operationLog.log(operation);
      contexts.put(id, new ORaftRequestContext(operation, quorum, finished));
      timer.schedule(new ORaftOperationTimeoutTimerTask(this, id), timeout, timeout);
      for (OStructuralDistributedMember value : members.values()) {
        value.propagate(id, operation);
      }
    });
  }

  public void receiveAck(ONodeIdentity node, OLogId id) {
    executor.execute(() -> {
      ORaftRequestContext ctx = contexts.get(id);
      if (ctx != null && ctx.ack(node, this)) {
        for (OStructuralDistributedMember value : members.values()) {
          value.confirm(id);
        }
        contexts.remove(id);
      }
    });
  }

  public void operationTimeout(OLogId id, TimerTask tt) {
    executor.execute(() -> {
      ORaftRequestContext ctx = contexts.get(id);
      if (ctx != null) {
        if (ctx.timeout()) {
          contexts.remove(id);
          //TODO: if an operation timedout, is should stop everything following raft.
          tt.cancel();
        }
      } else {
        tt.cancel();
      }
    });
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void receiveSubmit(ONodeIdentity senderNode, OSessionOperationId operationId, OStructuralSubmit request) {
    executor.execute(() -> {
      request.begin(operationId, this);
    });
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmit request) {
    executor.execute(() -> {
      request.begin(operationId, this);
    });
  }

  @Override
  public OrientDBDistributed getOrientDB() {
    return context;
  }

  public void join(OStructuralDistributedMember member) {
    lockManager.lockResource(CONF_RESOURCE, (guards) -> {
      submit(new OSessionOperationId(), (id, context) -> {
        OStructuralSharedConfiguration conf = context.getOrientDB().getStructuralConfiguration().getSharedConfiguration();
        if (!conf.existsNode(member.getIdentity()) && conf.canAddNode(member.getIdentity())) {
          this.propagateAndApply(new ONodeJoin(member.getIdentity()), () -> {
            lockManager.unlock(guards);
          });
        }
      });
    });
  }
}
