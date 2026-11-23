package com.orientechnologies.orient.distributed.context.coordination;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;

public class OPromisedDistributedOpsTest {

  private class DistMessage implements ODistributedMessage {

    private OTransactionIdPromise promise;

    public DistMessage(OTransactionIdPromise promise) {
      this.promise = promise;
    }

    @Override
    public OTransactionIdPromise getPromiseId() {
      return promise;
    }

    @Override
    public OOperationMessage getOp() {
      return null;
    }

    @Override
    public Optional<OAcceptResult> validate(OOperationContext ctx) {
      return Optional.empty();
    }

    @Override
    public void apply(OOperationContext ctx) {}

    @Override
    public void cancel(OOperationContext ctx) {}

    @Override
    public void recoordinate(OOperationContext ctx) {}
  }

  private ODistributedMessage newMsg() {
    var node = new ONodeId(UUID.randomUUID().toString());
    var promise = new OTransactionIdPromise(node, new OTransactionId(4, 10));
    return new DistMessage(promise);
  }

  @Test
  public void happyPath() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addPromised(msg);

    var read = promised.getPromised(msg.getPromiseId());
    assertSame(msg, read);

    assertNull(promised.getNotPromised(msg.getPromiseId()));
    assertTrue(promised.isPromised(msg.getPromiseId().getId()));

    promised.finalizeSuccess(msg.getPromiseId());

    assertNull(promised.getPromised(msg.getPromiseId()));
    assertFalse(promised.isPromised(msg.getPromiseId().getId()));
  }

  @Test
  public void notPromised() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addNotPromised(msg);

    var read = promised.getNotPromised(msg.getPromiseId());
    assertSame(msg, read);

    assertNull(promised.getPromised(msg.getPromiseId()));
    assertFalse(promised.isPromised(msg.getPromiseId().getId()));

    promised.cancel(msg.getPromiseId());

    assertNull(promised.getNotPromised(msg.getPromiseId()));
  }

  @Test
  public void cancelPromised() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addPromised(msg);

    var read = promised.getPromised(msg.getPromiseId());
    assertSame(msg, read);

    assertNull(promised.getNotPromised(msg.getPromiseId()));
    assertTrue(promised.isPromised(msg.getPromiseId().getId()));

    promised.cancel(msg.getPromiseId());

    assertNull(promised.getPromised(msg.getPromiseId()));
    assertFalse(promised.isPromised(msg.getPromiseId().getId()));
  }

  @Test
  public void confirmNotPromised() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addNotPromised(msg);

    var read = promised.getNotPromised(msg.getPromiseId());
    assertSame(msg, read);

    assertNull(promised.getPromised(msg.getPromiseId()));
    assertFalse(promised.isPromised(msg.getPromiseId().getId()));

    promised.finalizeSuccess(msg.getPromiseId());

    assertNull(promised.getNotPromised(msg.getPromiseId()));
  }

  @Test
  public void nodeDisconnectedPromised() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addPromised(msg);

    promised.nodeDisconnected(msg.getPromiseId().getCoordinator());

    assertNull(promised.getPromised(msg.getPromiseId()));
    assertFalse(promised.isPromised(msg.getPromiseId().getId()));
  }

  @Test
  public void nodeDisconnectedNotPromised() {
    var promised = new OPromisedDistributedOpsImpl();

    var msg = newMsg();

    promised.addNotPromised(msg);

    promised.nodeDisconnected(msg.getPromiseId().getCoordinator());
    assertNull(promised.getNotPromised(msg.getPromiseId()));
  }
}
