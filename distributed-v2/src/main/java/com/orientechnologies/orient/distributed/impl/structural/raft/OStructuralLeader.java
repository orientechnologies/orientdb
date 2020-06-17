package com.orientechnologies.orient.distributed.impl.structural.raft;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedLockManager;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.log.OOperationLogEntry;
import com.orientechnologies.orient.distributed.impl.log.OOplogIterator;
import com.orientechnologies.orient.distributed.impl.structural.OReadStructuralSharedConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.operations.OFullConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.submit.OCreateDatabaseSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.submit.ODropDatabaseSubmitResponse;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OStructuralLeader implements AutoCloseable, OLeaderContext {
  private static final String CONF_RESOURCE = "Configuration";
  private final ExecutorService executor;
  private final OOperationLog operationLog;
  private final ConcurrentMap<OLogId, ORaftRequestContext> contexts = new ConcurrentHashMap<>();
  private final Set<ONodeIdentity> members = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Timer timer;
  private final OrientDBDistributed context;
  private final ONodeIdentity nodeIdentity;
  private ODistributedLockManager lockManager = new ODistributedLockManagerImpl();
  private int quorum;
  private int timeout;
  private ODistributedNetwork network;

  public OStructuralLeader(
      OOperationLog operationLog, ODistributedNetwork network, OrientDBDistributed context) {
    this.executor = Executors.newSingleThreadExecutor();
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.context = context;
    this.quorum = context.getStructuralConfiguration().readSharedConfiguration().getQuorum();
    this.timeout =
        context
            .getConfigurations()
            .getConfigurations()
            .getValueAsInteger(DISTRIBUTED_TX_EXPIRE_TIMEOUT);
    long pingTimeout = 1000; // context.getConfigurations().getConfigurations().getValueAsInteger()
    this.nodeIdentity = context.getNodeIdentity();
    this.network = network;
    this.timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            OLogId logId = operationLog.lastPersistentLog();
            network.notifyLastStructuralOperation(context.getNodeIdentity(), logId);
          }
        },
        pingTimeout,
        pingTimeout);
  }

  public void propagateAndApply(ORaftOperation operation, OpFinished finished) {
    executor.execute(
        () -> {
          OLogId id = operationLog.log(operation);
          contexts.put(id, new ORaftRequestContext(operation, quorum, finished));
          timer.schedule(new ORaftOperationTimeoutTimerTask(this, id), timeout, timeout);
          Set<ONodeIdentity> to = new HashSet<>(members);
          to.remove(nodeIdentity);
          network.propagate(to, id, operation);
          receiveAck(getOrientDB().getStructuralConfiguration().getCurrentNodeIdentity(), id);
        });
  }

  public void receiveAck(ONodeIdentity node, OLogId id) {
    executor.execute(
        () -> {
          ORaftRequestContext ctx = contexts.get(id);
          if (ctx != null && ctx.ack(node, this)) {
            Set<ONodeIdentity> to = new HashSet<>(members);
            to.remove(nodeIdentity);
            network.confirm(to, id);
            contexts.remove(id);
          }
        });
  }

  public void operationTimeout(OLogId id, TimerTask tt) {
    executor.execute(
        () -> {
          ORaftRequestContext ctx = contexts.get(id);
          if (ctx != null) {
            if (ctx.timeout()) {
              contexts.remove(id);
              // TODO: if an operation timedout, is should stop everything following raft.
              tt.cancel();
            }
          } else {
            tt.cancel();
          }
        });
  }

  @Override
  public void close() {
    timer.cancel();
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void receiveSubmit(
      ONodeIdentity senderNode, OSessionOperationId operationId, OStructuralSubmit request) {
    if (members.contains(senderNode)) {
      executor.execute(
          () -> {
            request.begin(Optional.of(senderNode), operationId, this);
          });
    }
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmit request) {
    executor.execute(
        () -> {
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
    submit(
        new OSessionOperationId(),
        (requester, id, context) -> {
          getLockManager()
              .lockResource(
                  CONF_RESOURCE,
                  (guards) -> {
                    OReadStructuralSharedConfiguration conf =
                        context
                            .getOrientDB()
                            .getStructuralConfiguration()
                            .readSharedConfiguration();
                    if (!conf.existsNode(identity) && conf.canAddNode(identity)) {
                      this.propagateAndApply(
                          new ONodeJoin(identity),
                          () -> {
                            getLockManager().unlock(guards);
                          });
                    } else {
                      getLockManager().unlock(guards);
                    }
                  });
        });
  }

  @Override
  public void createDatabase(
      Optional<ONodeIdentity> requester,
      OSessionOperationId operationId,
      String database,
      String type,
      Map<String, String> configurations) {
    getLockManager()
        .lockResource(
            CONF_RESOURCE,
            (guards) -> {
              OReadStructuralSharedConfiguration shared =
                  getOrientDB().getStructuralConfiguration().readSharedConfiguration();
              if (shared.existsDatabase(database)) {
                getLockManager().unlock(guards);
                if (requester.isPresent()) {
                  network.reply(
                      requester.get(),
                      operationId,
                      new OCreateDatabaseSubmitResponse(false, "Database Already Exists"));
                }
              } else {
                this.propagateAndApply(
                    new OCreateDatabase(operationId, database, type, configurations),
                    () -> {
                      getLockManager().unlock(guards);
                      if (requester.isPresent()) {
                        network.reply(
                            requester.get(),
                            operationId,
                            new OCreateDatabaseSubmitResponse(true, ""));
                      }
                    });
              }
            });
  }

  @Override
  public void dropDatabase(
      Optional<ONodeIdentity> requester, OSessionOperationId operationId, String database) {
    getLockManager()
        .lockResource(
            CONF_RESOURCE,
            (guards) -> {
              OReadStructuralSharedConfiguration shared =
                  getOrientDB().getStructuralConfiguration().readSharedConfiguration();
              if (shared.existsDatabase(database)) {
                this.propagateAndApply(
                    new ODropDatabase(operationId, database),
                    () -> {
                      getLockManager().unlock(guards);
                      if (requester.isPresent()) {
                        network.reply(
                            requester.get(),
                            operationId,
                            new ODropDatabaseSubmitResponse(true, ""));
                      }
                    });
              } else {
                getLockManager().unlock(guards);
                if (requester.isPresent()) {
                  network.reply(
                      requester.get(),
                      operationId,
                      new ODropDatabaseSubmitResponse(false, "Database do not exists"));
                }
              }
            });
  }

  public void connected(ONodeIdentity identity) {
    OReadStructuralSharedConfiguration conf =
        context.getStructuralConfiguration().readSharedConfiguration();
    if (conf.existsNode(identity) || conf.canAddNode(identity)) {
      members.add(identity);
    }
  }

  @Override
  public boolean tryResend(ONodeIdentity identity, OLogId logId) {
    // TODO: this may fail, handle the failure.
    Optional<OOplogIterator> res = operationLog.searchFrom(logId);
    if (res.isPresent()) {
      Iterator<OOperationLogEntry> iter = res.get();
      executor.execute(
          () -> {
            // TODO: this in single thread executor may cost too much, find a different
            // implementation
            while (iter.hasNext()) {
              OOperationLogEntry logEntry = iter.next();
              network.propagate(
                  Collections.singleton(identity),
                  logEntry.getLogId(),
                  (ORaftOperation) logEntry.getRequest());
            }
          });
      return true;
    }
    return false;
  }

  @Override
  public void sendFullConfiguration(ONodeIdentity identity) {
    executor.execute(
        () -> {
          OStructuralConfiguration structuralConfiguration =
              getOrientDB().getStructuralConfiguration();
          OLogId lastId = structuralConfiguration.getLastUpdateId();
          OReadStructuralSharedConfiguration shared =
              structuralConfiguration.readSharedConfiguration();
          getOrientDB().getNetworkManager().send(identity, new OFullConfiguration(lastId, shared));
        });
  }

  public void disconnected(ONodeIdentity nodeIdentity) {
    members.remove(nodeIdentity);
  }
}
