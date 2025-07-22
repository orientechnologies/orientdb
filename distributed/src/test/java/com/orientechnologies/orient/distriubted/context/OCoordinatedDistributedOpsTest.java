package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequentialAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuorumNotReached;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class OCoordinatedDistributedOpsTest {

  private class TestAction implements OCompleteAction {
    private boolean success = false;
    private boolean failure = false;

    @Override
    public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {
      success = true;
    }

    @Override
    public void failure(OTransactionIdPromise promise, Set<ONodeId> expected) {
      failure = true;
    }
  }

  private ONodeId newRandomNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  @Test
  public void baseSuccess() {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseFail() {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.failure(nodeIdtwo, promise, new OInvalidSequentialAcceptResult());

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void baseSuccessMoreNodes() {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    assertFalse(action.success);
    ops.success(nodeIdthree, promise);
    assertTrue(action.success);
    ops.success(nodeIdFour, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseSuccessMoreNodesUregistered() {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour);

    ops.unregisterNode(nodeIdthree);
    ops.unregisterNode(nodeIdFour);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseFailureeNodesUregistered() throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.unregisterNode(nodeIdtwo);
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(start.result().get().get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseFailureeNodesUregisteredAndFailed()
      throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(2);
    ONodeId nodeId = newRandomNodeId();
    ops.registerNode(nodeId);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    ops.unregisterNode(nodeIdthree);
    ops.failure(nodeIdFour, promise, new OInvalidSequentialAcceptResult());
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(start.result().get().get() instanceof OQuorumNotReached);
  }
}
