package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
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

  public void execute(
      OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution);

  public record ORequestMergeAction(ONodeId requestToMerge) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      context.sendMergeOperation(requestToMerge, execution);
    }
  }

  public record ONotifySelf(Set<ONodeId> nodes) implements ODiscoverAction {
    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      context.sendFirstConnects(nodes);
      context.dumpNodeInfo();
    }
  }

  record OEstablishAction(OGroupId groupId, Set<ONodeId> candidates) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      context.sendEstablish(groupId(), candidates(), execution);
    }
  }

  record OMergeNodeAction(ONodeId node) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution exection) {
      context.sendMergeNodeAction(node, state, exection);
    }
  }

  record OAddNodeAction(ONodeId node) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution exection) {
      long version = context.getNodeState().getOps().nextTopologyVersion();
      context.coordinatedOperation(new OAddTopologyMember(version, node()), exection);
    }
  }

  record ONoneAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      // Noting to do
    }
  }

  record OApplyStateAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      context.autoDeployIfNeed();
      context.dumpNodeInfo();
    }
  }

  record OApplySequenceAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, ONodeStateNetwork state, OCompleteExecution execution) {
      context.autoDeployIfNeed();
      context.dumpNodeInfo();
    }
  }
}
