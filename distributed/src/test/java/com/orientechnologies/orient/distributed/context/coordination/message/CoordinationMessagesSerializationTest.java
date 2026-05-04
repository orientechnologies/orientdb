package com.orientechnologies.orient.distributed.context.coordination.message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
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
    OFailPropose succ = new OFailPropose(reply, id, new OInvalidSequential(0, 0));

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

    var groupId = newGroupId();
    ONodeId nodeId = newNodeId();
    OTopologyStateNetwork net =
        new OTopologyStateNetwork(
            groupId, OTopologyState.BOOT, new HashSet<>(), 0, new OVersion(0));
    var sequenceStatus = new OTransactionSequenceStatus(new long[] {10, 20, 30});

    ONodeStateNetwork network = new ONodeStateNetwork(net, Collections.emptyList(), sequenceStatus);
    ONodeFirstConnect succ = new ONodeFirstConnect(nodeId, network, false);

    ONodeFirstConnect read = writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    OTopologyStateNetwork topology = read.getState().topology();
    assertEquals(topology.state(), OTopologyState.BOOT);
    assertEquals(topology.version().getValue(), 0);
    assertTrue(topology.members().isEmpty());
    Set<ONodeId> nodes = Set.of(newNodeId(), newNodeId());
    net =
        new OTopologyStateNetwork(groupId, OTopologyState.ESTABLISHED, nodes, 2, new OVersion(10));
    network = new ONodeStateNetwork(net, Collections.emptyList(), sequenceStatus);
    succ = new ONodeFirstConnect(nodeId, network, false);

    read = writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    OTopologyStateNetwork topology2 = read.getState().topology();
    assertEquals(topology2.state(), OTopologyState.ESTABLISHED);
    assertEquals(topology2.version().getValue(), 10);
    assertEquals(topology2.members(), nodes);
    assertEquals(topology2.groupId(), groupId);
  }

  @Test
  public void syncRequestTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    OSyncId syncId = new OSyncId(dbId, nodeId);
    OSyncRequest syncReq = new OSyncRequest(nodeId, dbId, syncId, new OSyncMode.BlockingBackup());

    OSyncRequest read = writeRead(syncReq);
    assertEquals(read.getFrom(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getMode(), new OSyncMode.BlockingBackup());
  }

  @Test
  public void canSyncTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    OSyncId syncId = new OSyncId(dbId, nodeId);
    OCanSync syncReq = new OCanSync(nodeId, dbId, syncId, new OCanSyncAccept.BlockingSync());

    OCanSync read = writeRead(syncReq);
    assertEquals(read.getSender(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getCanSync(), new OCanSyncAccept.BlockingSync());
  }

  @Test
  public void startSyncTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    OSyncId syncId = new OSyncId(dbId, nodeId);
    OStartSync syncReq = new OStartSync(nodeId, dbId, syncId, new OCanSyncAccept.BlockingSync());

    OStartSync read = writeRead(syncReq);
    assertEquals(read.getReceiver(), nodeId);
    assertEquals(read.getDbId(), dbId);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getMode(), new OCanSyncAccept.BlockingSync());
  }

  @Test
  public void syncDataTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    OSyncId syncId = new OSyncId(dbId, nodeId);
    var data = new byte[] {0, 1, 2};
    OSyncData syncReq = new OSyncData(syncId, data, 10, true);

    OSyncData read = writeRead(syncReq);

    assertEquals(read.getSyncId(), syncId);
    assertEquals(read.getSequential(), 10);
    assertArrayEquals(read.getData(), data);
    assertTrue(syncReq.isFinished());
  }

  @Test
  public void nextBufferTest() throws IOException {
    ONodeId nodeId = newNodeId();
    ODatabaseId dbId = newDatabaseId();
    OSyncId syncId = new OSyncId(dbId, nodeId);
    ONextBuffer syncReq = new ONextBuffer(syncId, true);

    ONextBuffer read = writeRead(syncReq);
    assertEquals(read.getSyncId(), syncId);
    assertTrue(read.isClose());
  }

  private ONodeStateNetwork newNetworkState(ONodeId nodeId1, OGroupId groupId) {
    OTopologyStateNetwork topology =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1), 1, new OVersion(1));
    OTransactionSequenceStatus status = new OTransactionSequenceStatus(new long[] {});
    return new ONodeStateNetwork(topology, Collections.emptyList(), status);
  }

  @Test
  public void mergeRequestTest() throws IOException {
    var promise = newPromiseId();
    var groupId = newGroupId();
    var state = newNetworkState(newNodeId(), groupId);
    var original = newNetworkState(newNodeId(), groupId);
    OMergeRequest merge = new OMergeRequest(promise, groupId, state, original);
    OMergeRequest read = writeRead(merge);
    assertEquals(promise, read.getPromise());
    assertEquals(groupId, read.getGroup());
  }

  @Test
  public void mergeResultTest() throws IOException {
    var node = newNodeId();
    var promise = newPromiseId();
    OMergeResult merge = new OMergeResult(node, promise, Optional.empty());
    OMergeResult read = writeRead(merge);
    assertEquals(node, read.getNode());
    assertEquals(promise, read.getPromise());
    assertTrue(read.getAccepted().isEmpty());
  }

  @Test
  public void topologyPingTest() throws IOException {
    var node = newNodeId();
    var status = new OTransactionSequenceStatus(new long[] {1});
    OTopologyPing ping = new OTopologyPing(node, status);
    OTopologyPing read = writeRead(ping);
    assertEquals(read.getNodeId(), node);
    assertEquals(read.getStatus(), status);
  }

  @Test
  public void retryProposeTest() throws IOException {
    var promise = newPromiseId();
    var version = new OVersion(0);
    var dbId = newDatabaseId();
    var operation = new ODropDbMessage(dbId, version);
    ORetryProposeOp op = new ORetryProposeOp(promise, operation);
    ORetryProposeOp read = writeRead(op);
    assertTrue(read.getOp() instanceof ODropDbMessage);
    assertEquals(read.getPromise(), promise);
  }
}
