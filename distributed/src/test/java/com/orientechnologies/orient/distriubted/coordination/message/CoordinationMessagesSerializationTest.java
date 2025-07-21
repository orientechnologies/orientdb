package com.orientechnologies.orient.distriubted.coordination.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailPropose;
import com.orientechnologies.orient.distributed.context.coordination.message.OInvalidSequentialAcceptResutl;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSuccessPropose;
import com.orientechnologies.orient.distributed.db.ODropDbMessage;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.junit.Test;

public class CoordinationMessagesSerializationTest {

  private ONodeId newNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private OTransactionIdPromise newPromiseId() {
    return new OTransactionIdPromise(newNodeId(), new OTransactionId(10, 20));
  }

  private OStructuralMessage writeRead(OStructuralMessage message) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);

    message.writeNetwork(out);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInput input = new DataInputStream(inputStream);
    return OStructuralMessage.readNetwork(input);
  }

  @Test
  public void propose() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OProposeOp propose = new OProposeOp(id, new ODropDbMessage("db-name"));

    OProposeOp read = (OProposeOp) writeRead(propose);

    assertEquals(read.getPromiseId(), id);
    OOperationMessage operation = read.getOp();

    assertEquals(((ODropDbMessage) operation).getName(), "db-name");
  }

  @Test
  public void successPropose() throws IOException {

    ONodeId reply = newNodeId();
    OTransactionIdPromise id = newPromiseId();
    OSuccessPropose succ = new OSuccessPropose(reply, id);

    OSuccessPropose read = (OSuccessPropose) writeRead(succ);

    assertEquals(read.getPromise(), id);
    assertEquals(read.getNodeId(), reply);
  }

  @Test
  public void failPropose() throws IOException {

    ONodeId reply = newNodeId();
    OTransactionIdPromise id = newPromiseId();
    OFailPropose succ = new OFailPropose(reply, id, new OInvalidSequentialAcceptResutl());

    OFailPropose read = (OFailPropose) writeRead(succ);

    assertEquals(read.getPromise(), id);
    assertEquals(read.getNodeId(), reply);
    assertTrue(read.getAcceptResult() instanceof OInvalidSequentialAcceptResutl);
  }

  @Test
  public void confirmOp() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OConfirmOp succ = new OConfirmOp(id);

    OConfirmOp read = (OConfirmOp) writeRead(succ);

    assertEquals(read.getPromise(), id);
  }

  @Test
  public void failOp() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OFailOp succ = new OFailOp(id);

    OFailOp read = (OFailOp) writeRead(succ);

    assertEquals(read.getPromise(), id);
  }
}
