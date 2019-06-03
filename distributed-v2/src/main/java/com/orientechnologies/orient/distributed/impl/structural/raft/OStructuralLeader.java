package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OReadStructuralSharedConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.operations.OCreateDatabaseSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.operations.ODropDatabaseSubmitResponse;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT;

public class OStructuralLeader implements AutoCloseable, OLeaderContext {
  private static final String                                     CONF_RESOURCE = "Configuration";
  private final        ExecutorService                            executor;
  private final        OOperationLog                              operationLog;
  private final        ConcurrentMap<OLogId, ORaftRequestContext> contexts      = new ConcurrentHashMap<>();
  private final        Map<ONodeIdentity, ODistributedChannel>    members       = new ConcurrentHashMap<>();
  private final        Timer                                      timer;
  private final        OrientDBDistributed                        context;
  private              ODistributedLockManager                    lockManager   = new ODistributedLockManagerImpl();
  private              int                                        quorum;
  private              int                                        timeout;

  public OStructuralLeader(ExecutorService executor, OOperationLog operationLog, OrientDBDistributed context) {
    this.executor = executor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.context = context;
    this.quorum = context.getStructuralConfiguration().readSharedConfiguration().getQuorum();
    this.timeout = context.getConfigurations().getConfigurations().getValueAsInteger(DISTRIBUTED_TX_EXPIRE_TIMEOUT);
  }

  public void propagateAndApply(ORaftOperation operation, OpFinished finished) {
    executor.execute(() -> {
      OLogId id = operationLog.log(operation);
      contexts.put(id, new ORaftRequestContext(operation, quorum, finished));
      timer.schedule(new ORaftOperationTimeoutTimerTask(this, id), timeout, timeout);
      for (ODistributedChannel value : members.values()) {
        value.propagate(id, operation);
      }
      receiveAck(getOrientDB().getStructuralConfiguration().getCurrentNodeIdentity(), id);
    });
  }

  public void receiveAck(ONodeIdentity node, OLogId id) {
    executor.execute(() -> {
      ORaftRequestContext ctx = contexts.get(id);
      if (ctx != null && ctx.ack(node, this)) {
        for (ODistributedChannel value : members.values()) {
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
      request.begin(Optional.of(senderNode), operationId, this);
    });
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmit request) {
    executor.execute(() -> {
      request.begin(Optional.empty(), operationId, this);
    });
  }

  @Override
  public OrientDBDistributed getOrientDB() {
    return context;
  }

  public ODistributedLockManager getLockManager() {
    return lockManager;
  }

  public void join(ONodeIdentity identity) {
    submit(new OSessionOperationId(), (requester, id, context) -> {
      getLockManager().lockResource(CONF_RESOURCE, (guards) -> {
        OReadStructuralSharedConfiguration conf = context.getOrientDB().getStructuralConfiguration().readSharedConfiguration();
        if (!conf.existsNode(identity) && conf.canAddNode(identity)) {
          this.propagateAndApply(new ONodeJoin(identity), () -> {
            getLockManager().unlock(guards);
          });
        } else {
          getLockManager().unlock(guards);
        }
      });
    });
  }

  @Override
  public void createDatabase(Optional<ONodeIdentity> requester, OSessionOperationId operationId, String database, String type,
      Map<String, String> configurations) {
    getLockManager().lockResource(CONF_RESOURCE, (guards) -> {
      OReadStructuralSharedConfiguration shared = getOrientDB().getStructuralConfiguration().readSharedConfiguration();
      if (shared.existsDatabase(database)) {
        getLockManager().unlock(guards);
        if (requester.isPresent()) {
          ODistributedChannel requesterChannel = members.get(requester.get());
          requesterChannel.reply(operationId, new OCreateDatabaseSubmitResponse(false, "Database Already Exists"));
        }
      } else {
        this.propagateAndApply(new OCreateDatabase(operationId, database, type, configurations), () -> {
          getLockManager().unlock(guards);
          if (requester.isPresent()) {
            ODistributedChannel requesterChannel = members.get(requester.get());
            requesterChannel.reply(operationId, new OCreateDatabaseSubmitResponse(true, ""));
          }
        });
      }
    });
  }

  @Override
  public void dropDatabase(Optional<ONodeIdentity> requester, OSessionOperationId operationId, String database) {
    getLockManager().lockResource(CONF_RESOURCE, (guards) -> {
      OReadStructuralSharedConfiguration shared = getOrientDB().getStructuralConfiguration().readSharedConfiguration();
      if (shared.existsDatabase(database)) {
        this.propagateAndApply(new ODropDatabase(operationId, database), () -> {
          getLockManager().unlock(guards);
          if (requester.isPresent()) {
            ODistributedChannel requesterChannel = members.get(requester.get());
            requesterChannel.reply(operationId, new ODropDatabaseSubmitResponse(true, ""));
          }
        });
      } else {
        getLockManager().unlock(guards);
        if (requester.isPresent()) {
          ODistributedChannel requesterChannel = members.get(requester.get());
          requesterChannel.reply(operationId, new ODropDatabaseSubmitResponse(false, "Database do not exists"));
        }
      }
    });
  }

  public void connected(ONodeIdentity identity, ODistributedChannel channel) {
    OReadStructuralSharedConfiguration conf = context.getStructuralConfiguration().readSharedConfiguration();
    if (conf.existsNode(identity) || conf.canAddNode(identity)) {
      members.put(identity, channel);
    }
  }

  @Override
  public void tryResend(ONodeIdentity identity, OLogId logId) {
    //TODO: this may fail, handle the failure.
    executor.execute(() -> {
      //TODO: this in single thread executor may cost too much, find a different implementation
      Iterator<OOperationLogEntry> iter = operationLog.iterate(logId, operationLog.lastPersistentLog());
      while (iter.hasNext()) {
        OOperationLogEntry logEntry = iter.next();
        members.get(identity).propagate(logEntry.getLogId(), (ORaftOperation) logEntry.getRequest());
      }
    });
  }

  @Override
  public void sendFullConfiguration(ONodeIdentity identity) {
    executor.execute(() -> {
      OStructuralConfiguration structuralConfiguration = getOrientDB().getStructuralConfiguration();
      OLogId lastId = structuralConfiguration.getLastUpdateId();
      OReadStructuralSharedConfiguration shared = structuralConfiguration.readSharedConfiguration();
      members.get(identity).send(new OFullConfiguration(lastId, shared));
    });
  }

  public void disconnected(ONodeIdentity nodeIdentity) {
    members.remove(nodeIdentity);
  }
}
