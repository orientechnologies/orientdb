package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.db.OStandardCompleteAction;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Set;

public sealed interface ODiscoverAction
    permits ODiscoverAction.OEstablishAction,
        ODiscoverAction.OAddNodeAction,
        ODiscoverAction.ONoneAction {

  void execute(OrientDBDistributed context);

  record OEstablishAction(OGroupId groupId, Set<ONodeId> candidates) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context) {
      OTransactionIdPromise promise =
          context
              .getNodeState()
              .startEnstablish(this.candidates(), new OStandardCompleteAction(context));
      OEnstablishTopology operation = new OEnstablishTopology(groupId(), candidates());
      context.sendMessage(candidates(), new OProposeOp(promise, operation));
    }
  }

  record OAddNodeAction(ONodeId node, long version) implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context) {
      context.distributedOperation(new OAddTopologyMember(version(), node()));
    }
  }

  record ONoneAction() implements ODiscoverAction {

    @Override
    public void execute(OrientDBDistributed context) {
      // Noting to do
    }
  }
}
