package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEnstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.result.ONoTransactionSequencialAvailable;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
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

  public void execute(
      OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState);

  default boolean applyDatabaseState() {
    return false;
  }

  default boolean applySequenceState() {
    return false;
  }

  public record OMergeAction(Set<ONodeId> members) implements ODiscoverAction {
    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {
      context.sendMergeOperation(members, execution, otherState);
    }
  }

  public record ONotifySelf(Set<ONodeId> nodes) implements ODiscoverAction {
    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {
      context.sendFirstConnects(nodes);
    }
  }

  record OEstablishAction(OGroupId groupId, Set<ONodeId> candidates) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {
      OEnstablishTopology operation = new OEnstablishTopology(groupId(), candidates());
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

  record OAddNodeAction(ONodeId node, long version, boolean merge) implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution exection, ONodeStateNetwork otherState) {
      context.coordinatedOperation(new OAddTopologyMember(version(), node()), exection);
    }

    @Override
    public boolean applyDatabaseState() {
      return merge;
    }

    @Override
    public boolean applySequenceState() {
      return merge;
    }
  }

  record ONoneAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {
      // Noting to do
    }
  }

  record OApplyStateAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {}

    @Override
    public boolean applyDatabaseState() {
      return true;
    }
  }

  record OApplySequenceAction() implements ODiscoverAction {

    @Override
    public void execute(
        OrientDBDistributed context, OCompleteExecution execution, ONodeStateNetwork otherState) {
      context.autoDeployIfNeed();
    }

    @Override
    public boolean applyDatabaseState() {
      return true;
    }

    @Override
    public boolean applySequenceState() {
      return true;
    }
  }
}
