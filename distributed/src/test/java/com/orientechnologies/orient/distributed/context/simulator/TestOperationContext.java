package com.orientechnologies.orient.distributed.context.simulator;

import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import java.util.Set;

public class TestOperationContext implements OOperationContext {

  private OCoordinatedDistributedOps ops;

  public TestOperationContext(OCoordinatedDistributedOps ops) {
    this.ops = ops;
  }

  @Override
  public OCoordinatedDistributedOps getOps() {
    return ops;
  }

  @Override
  public void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    ops.mergeNode(node, state, original, promise);
  }

  @Override
  public void establish(OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    ops.establish(groupId, candidates, promise);
  }

  @Override
  public void distributedDrop(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    ops.dropDatabase(dbId, version, promise);
  }

  @Override
  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId id,
      String name,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    ops.declareDatabase(promise, id, name, partecipants, minimumQuorum);
  }

  @Override
  public void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise) {
    ops.registerNode(node, version, promise);
  }

  @Override
  public void recoordinateOperation(OTransactionIdPromise promise, OOperationMessage op) {
    // TODO Auto-generated method stub
  }

  @Override
  public void apllied(OTransactionIdPromise promise) {
    ops.completeExecution(promise);
  }
}
