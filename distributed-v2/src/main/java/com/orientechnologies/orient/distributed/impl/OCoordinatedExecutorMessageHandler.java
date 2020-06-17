package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OCoordinatedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitContext;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralFollower;
import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralLeader;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;

public class OCoordinatedExecutorMessageHandler implements OCoordinatedExecutor {
  private OrientDBDistributed distributed;
  private ONodeIdentity leader;

  public OCoordinatedExecutorMessageHandler(OrientDBDistributed distributed) {
    this.distributed = distributed;
  }

  private void checkDatabaseReady(String database) {
    distributed.checkDatabaseReady(database);
  }

  @Override
  public void executeOperationRequest(
      ONodeIdentity sender, String database, OLogId id, ONodeRequest request) {
    checkDatabaseReady(database);
    ODistributedContext distributedContext = distributed.getDistributedContext(database);
    if (distributedContext != null) {
      ODistributedExecutor executor = distributedContext.getExecutor();
      executor.receive(sender, id, request);
    }
  }

  @Override
  public void executeOperationResponse(
      ONodeIdentity sender, String database, OLogId id, ONodeResponse response) {
    checkDatabaseReady(database);
    ODistributedContext distributedContext = distributed.getDistributedContext(database);
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance()
          .error(
              this,
              "Received coordinator response on a node that is not a coordinator ignoring it",
              null);
    } else {
      coordinator.receive(sender, id, response);
    }
  }

  @Override
  public void executeSubmitResponse(
      ONodeIdentity sender,
      String database,
      OSessionOperationId operationId,
      OSubmitResponse response) {
    checkDatabaseReady(database);
    ODistributedContext distributedContext = distributed.getDistributedContext(database);
    OSubmitContext context = distributedContext.getSubmitContext();
    context.receive(operationId, response);
  }

  @Override
  public void executeSubmitRequest(
      ONodeIdentity sender,
      String database,
      OSessionOperationId operationId,
      OSubmitRequest request) {
    checkDatabaseReady(database);
    ODistributedContext distributedContext = distributed.getDistributedContext(database);
    ODistributedCoordinator coordinator = distributedContext.getCoordinator();
    if (coordinator == null) {
      OLogManager.instance()
          .error(
              this,
              "Received submit request on a node that is not a coordinator ignoring it",
              null);
    } else {
      coordinator.submit(sender, operationId, request);
    }
  }

  @Override
  public void executeStructuralSubmitRequest(
      ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitRequest request) {
    OStructuralDistributedContext distributedContext =
        distributed.getStructuralDistributedContext();
    distributedContext.execute(sender, id, request);
  }

  @Override
  public void executeStructuralSubmitResponse(
      ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitResponse response) {
    OStructuralDistributedContext distributedContext =
        distributed.getStructuralDistributedContext();
    OStructuralSubmitContext context = distributedContext.getSubmitContext();
    context.receive(id, response);
  }

  @Override
  public void executePropagate(ONodeIdentity sender, OLogId id, ORaftOperation operation) {
    if (!sender.equals(leader)) {
      OLogManager.instance()
          .warn(
              this,
              "Received propagate from node '%s' but leader is '%s' ignoring it",
              sender,
              leader);
      return;
    }
    OStructuralDistributedContext distributedContext =
        distributed.getStructuralDistributedContext();
    OStructuralFollower follower = distributedContext.getFollower();
    follower.log(sender, id, operation);
  }

  @Override
  public void executeConfirm(ONodeIdentity sender, OLogId id) {
    if (!sender.equals(leader)) {
      OLogManager.instance()
          .warn(
              this,
              "Received confirm from node '%s' but leader is '%s' ignoring it",
              sender,
              leader);
      return;
    }
    OStructuralDistributedContext distributedContext =
        distributed.getStructuralDistributedContext();
    OStructuralFollower slave = distributedContext.getFollower();
    slave.confirm(id);
  }

  @Override
  public void executeAck(ONodeIdentity sender, OLogId id) {
    OStructuralDistributedContext distributedContext =
        distributed.getStructuralDistributedContext();
    OStructuralLeader master = distributedContext.getLeader();
    if (master == null) {
      OLogManager.instance()
          .error(
              this,
              "Received coordinator response on a node that is not a coordinator ignoring it",
              null);
    } else {
      master.receiveAck(sender, id);
    }
  }

  @Override
  public void nodeConnected(ONodeIdentity identity) {
    distributed.nodeConnected(identity);
  }

  @Override
  public void nodeDisconnected(ONodeIdentity identity) {
    distributed.nodeDisconnected(identity);
  }

  @Override
  public void setLeader(ONodeIdentity leader, OLogId leaderLastValid) {
    this.leader = leader;
    if (distributed.getNodeIdentity().equals(leader)) {
      distributed
          .getStructuralDistributedContext()
          .makeLeader(leader, distributed.getActiveNodes());
    } else {
      distributed.getStructuralDistributedContext().setExternalLeader(leader, leaderLastValid);
    }
  }

  @Override
  public void setDatabaseLeader(ONodeIdentity leader, String database, OLogId leaderLastValid) {
    ODistributedContext context = distributed.getDistributedContext(database);
    if (distributed.getNodeIdentity().equals(leader)) {
      context.makeCoordinator(leader, database, leaderLastValid, distributed.getActiveNodes());
    } else {
      context.setExternalCoordinator(leader, leaderLastValid);
    }
  }

  @Override
  public void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid) {
    distributed.getStructuralDistributedContext().getFollower().ping(leader, leaderLastValid);
  }

  @Override
  public void notifyLastDatabaseOperation(
      ONodeIdentity leader, String database, OLogId leaderLastValid) {
    distributed
        .getDistributedContext(database)
        .getExecutor()
        .notifyLastValidLog(leader, leaderLastValid);
  }

  @Override
  public void executeOperation(ONodeIdentity sender, OOperation operation) {
    operation.apply(sender, distributed);
  }
}
