package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.HashSet;
import java.util.Set;

public sealed interface ODiscoverAction
    permits ODiscoverAction.OEstablishAction,
        ODiscoverAction.OAddNodeAction,
        ODiscoverAction.ONoneAction,
        ODiscoverAction.ONotifySelf,
        ODiscoverAction.ORequestMergeAction,
        ODiscoverAction.OApplyStateAction,
        ODiscoverAction.OApplySequenceAction,
        ODiscoverAction.OMergeNodeAction {

  public void execute(OrientDBDistributed context, OCompleteExecution execution);

  default ODiscoverAction checkAndApply(
      OCoordinatedDistributedOpsImpl ops, ONodeStateNetwork state, boolean merge) {
    return this;
  }

  public record ORequestMergeAction(ONodeId requestToMerge) implements ODiscoverAction {
    private static final OLogger logger = OLogManager.instance().logger(ORequestMergeAction.class);

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution execution) {
      context.sendMergeOperation(requestToMerge, execution);
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
      context.sendEstablish(groupId(), candidates(), execution);
    }
  }

  record OMergeNodeAction(ONodeId node) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution exection) {
      context.sendMergeNodeAction(node, exection);
    }
  }

  record OAddNodeAction(ONodeId node) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context, OCompleteExecution exection) {
      long version = context.getNodeState().getOps().nextTopologyVersion();
      context.coordinatedOperation(new OAddTopologyMember(version, node()), exection);
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
