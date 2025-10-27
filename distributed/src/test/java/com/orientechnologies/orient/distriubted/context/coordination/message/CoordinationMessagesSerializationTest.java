package com.orientechnologies.orient.distriubted.context.coordination.message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailPropose;
import com.orientechnologies.orient.distributed.context.coordination.message.ONextBuffer;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeFirstConnect;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.OStartSync;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSuccessPropose;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncData;
import com.orientechnologies.orient.distributed.context.coordination.message.OSyncRequest;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class CoordinationMessagesSerializationTest {

  private ONodeId newNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private OTransactionIdPromise newPromiseId() {
    return new OTransactionIdPromise(newNodeId(), new OTransactionId(10, 20));
  }

  private OGroupId newGroupId() {
    return new OGroupId("netId");
  }

  private ODatabaseId newDatabaseId() {
    return new ODatabaseId("dbID");
  }

  private <T extends OStructuralMessage> T writeRead(T message) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);

    message.writeNetwork(out);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInput input = new DataInputStream(inputStream);
    return (T) OStructuralMessage.readNetwork(input);
  }

  @Test
  public void successPropose() throws IOException {

    ONodeId reply = newNodeId();
    OTransactionIdPromise id = newPromiseId();
    OSuccessPropose succ = new OSuccessPropose(reply, id);

    OSuccessPropose read = writeRead(succ);

    assertEquals(read.getPromise(), id);
    assertEquals(read.getNodeId(), reply);
  }

  @Test
  public void failPropose() throws IOException {

    ONodeId reply = newNodeId();
    OTransactionIdPromise id = newPromiseId();
    OFailPropose succ = new OFailPropose(reply, id, new OInvalidSequential());

    OFailPropose read = writeRead(succ);

    assertEquals(read.getPromise(), id);
    assertEquals(read.getNodeId(), reply);
    assertTrue(read.getAcceptResult() instanceof OInvalidSequential);
  }

  @Test
  public void confirmOp() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OConfirmOp succ = new OConfirmOp(id);

    OConfirmOp read = writeRead(succ);

    assertEquals(read.getPromise(), id);
  }

  @Test
  public void failOp() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OFailOp succ = new OFailOp(id);

    OFailOp read = writeRead(succ);

    assertEquals(read.getPromise(), id);
  }

  @Test
  public void firstConnectTest() throws IOException {

    ONodeId nodeId = newNodeId();
    ONodeStateNetwork net =
        new ONodeStateNetwork(Optional.empty(), OTopologyState.BOOT, new HashSet<>(), 0, 0);
    ONodeFirstConnect succ = new ONodeFirstConnect(nodeId, net);

    ONodeFirstConnect read = writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    assertEquals(read.getState().getState(), OTopologyState.BOOT);
    assertEquals(read.getState().getVersion(), 0);
    assertTrue(read.getState().getGroupId().isEmpty());
    assertTrue(read.getState().getMembers().isEmpty());
    Set<ONodeId> nodes = Set.of(newNodeId(), newNodeId());
    var groupId = newGroupId();
    net = new ONodeStateNetwork(Optional.of(groupId), OTopologyState.ESTABLISHED, nodes, 2, 10);
    succ = new ONodeFirstConnect(nodeId, net);

    read = writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    assertEquals(read.getState().getState(), OTopologyState.ESTABLISHED);
    assertEquals(read.getState().getVersion(), 10);
    assertEquals(read.getState().getMembers(), nodes);
    assertEquals(read.getState().getGroupId().get(), groupId);
  }

  @Test
  public void syncRequestTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    UUID syncId = UUID.randomUUID();
    OSyncRequest syncReq = new OSyncRequest(nodeId, dbId, syncId, OSyncMode.StandardBackup);

    OSyncRequest read = writeRead(syncReq);
    assertEquals(read.getFrom(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getMode(), OSyncMode.StandardBackup);
  }

  @Test
  public void canSyncTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    UUID syncId = UUID.randomUUID();
    OCanSync syncReq = new OCanSync(nodeId, dbId, syncId, OSyncMode.StandardBackup, true);

    OCanSync read = writeRead(syncReq);
    assertEquals(read.getFrom(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getMode(), OSyncMode.StandardBackup);
    assertTrue(read.isCanSync());
  }

  @Test
  public void startSyncTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    UUID syncId = UUID.randomUUID();
    OStartSync syncReq = new OStartSync(nodeId, dbId, syncId, OSyncMode.StandardBackup);

    OStartSync read = writeRead(syncReq);
    assertEquals(read.getFrom(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getMode(), OSyncMode.StandardBackup);
  }

  @Test
  public void syncDataTest() throws IOException {
    UUID syncId = UUID.randomUUID();
    var data = new byte[] {0, 1, 2};
    OSyncData syncReq = new OSyncData(syncId, data);

    OSyncData read = writeRead(syncReq);

    assertEquals(read.getSyncId(), syncId);
    assertArrayEquals(read.getData(), data);
  }

  @Test
  public void nextBufferTest() throws IOException {
    UUID syncId = UUID.randomUUID();
    ONextBuffer syncReq = new ONextBuffer(syncId);

    ONextBuffer read = writeRead(syncReq);
    assertEquals(read.getSyncId(), syncId);
  }
}
