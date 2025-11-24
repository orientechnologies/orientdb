package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ORetryInfo;
import com.orientechnologies.orient.distributed.context.ORetryOperation;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEnstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;
import java.util.Set;

public sealed interface ODiscoverAction extends ORetryOperation
    permits ODiscoverAction.OEstablishAction,
        ODiscoverAction.OAddNodeAction,
        ODiscoverAction.ONoneAction,
        ODiscoverAction.ONotifySelf {

  public record ONotifySelf(Set<ONodeId> nodes) implements ODiscoverAction {
    @Override
    public Optional<OAcceptResult> execute(OrientDBDistributed context, ORetryInfo retry) {
      context.sendFirstConnects(nodes);
      return Optional.empty();
    }
  }

  record OEstablishAction(OGroupId groupId, Set<ONodeId> candidates) implements ODiscoverAction {

    @Override
    public Optional<OAcceptResult> execute(OrientDBDistributed context, ORetryInfo retry) {
      OEnstablishTopology operation = new OEnstablishTopology(groupId(), candidates());
      OTransactionIdPromise promise =
          context
              .getNodeState()
              .startEnstablish(this.candidates(), context.newCompleteAction(operation, retry));
      context.sendMessage(candidates(), new OProposeOp(promise, operation));
      return Optional.empty();
    }
  }

  record OAddNodeAction(ONodeId node, long version) implements ODiscoverAction {

    @Override
    public Optional<OAcceptResult> execute(OrientDBDistributed context, ORetryInfo retry) {
      return context.resultOperation(new OAddTopologyMember(version(), node()), retry);
    }
  }

  record ONoneAction() implements ODiscoverAction {

    @Override
    public Optional<OAcceptResult> execute(OrientDBDistributed context, ORetryInfo retry) {
      // Noting to do
      return Optional.empty();
    }
  }
}
