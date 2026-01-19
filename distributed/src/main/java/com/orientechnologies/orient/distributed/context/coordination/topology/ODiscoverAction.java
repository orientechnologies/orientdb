package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.ONoTransactionSequencialAvailable;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public sealed interface ODiscoverAction
    permits ODiscoverAction.OEstablishAction,
        ODiscoverAction.OAddNodeAction,
        ODiscoverAction.ONoneAction,
        ODiscoverAction.ONotifySelf,
        ODiscoverAction.OMergeAction,
        ODiscoverAction.OApplyStateAction,
        ODiscoverAction.OApplySequenceAction {

  public void execute(OrientDBDistributed context, OCompleteExecution execution);

  default ODiscoverAction checkAndApply(
      OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
    return this;
  }

  public record OMergeAction(Set<ONodeId> members) implements ODiscoverAction {
    private static final OLogger logger = OLogManager.instance().logger(OMergeAction.class);

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      context.sendMergeOperation(members, execution);
    }

    @Override
    public ODiscoverAction checkAndApply(
        OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
      var otherDbs = new HashSet<>(state.databases().stream().map((x) -> x.id()).toList());
      otherDbs.removeAll(ops.getDatabaseTopology().getDatabases());
      if (otherDbs.isEmpty()) {
        return this;
      } else {
        logger.warn("found join-able network, but can't merge into it with databases");
        return new ONoneAction();
      }
    }
  }

  public record ONotifySelf(Set<ONodeId> nodes) implements ODiscoverAction {
    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      context.sendFirstConnects(nodes);
    }
  }

  record OEstablishAction(OGroupId groupId, Set<ONodeId> candidates) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      OEstablishTopology operation = new OEstablishTopology(groupId(), candidates());
      Optional<OTransactionIdPromise> promise =
          context
              .getNodeState()
              .getOps()
              .startEstablish(this.candidates(), context.newCompleteAction(operation, execution));
      if (promise.isPresent()) {
        context.sendMessage(candidates(), new OProposeOp(promise.get(), operation));
      } else {
        execution.complete(Optional.of(new ONoTransactionSequencialAvailable()));
      }
    }
  }

  record OAddNodeAction(ONodeId node, boolean merge) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution exection) {
      long version = context.getNodeState().getOps().nextTopologyVersion();
      context.coordinatedOperation(new OAddTopologyMember(version, node()), exection);
    }

    @Override
    public ODiscoverAction checkAndApply(
        OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
      if (this.merge) {
        if (merge) {
          ops.getDatabaseTopology().mergeNetworkState(state.databases());
        } else {
          ops.getDatabaseTopology().receiverNetworkState(state.databases());
        }
        ops.getSequenceManager().fill(state.sequenceStatus());
        ops.notifyUpdate();
      }
      return this;
    }
  }

  record ONoneAction() implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      // Noting to do
    }
  }

  record OApplyStateAction() implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      context.autoDeployIfNeed();
    }

    @Override
    public ODiscoverAction checkAndApply(
        OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
      if (merge) {
        ops.getDatabaseTopology().mergeNetworkState(state.databases());
      } else {
        ops.getDatabaseTopology().receiverNetworkState(state.databases());
      }
      ops.notifyUpdate();
      return this;
    }
  }

  record OApplySequenceAction() implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      context.autoDeployIfNeed();
    }

    @Override
    public ODiscoverAction checkAndApply(
        OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
      if (merge) {
        ops.getDatabaseTopology().mergeNetworkState(state.databases());
      } else {
        ops.getDatabaseTopology().receiverNetworkState(state.databases());
      }
      ops.getSequenceManager().fill(state.sequenceStatus());
      ops.notifyUpdate();
      return this;
    }
  }
}
