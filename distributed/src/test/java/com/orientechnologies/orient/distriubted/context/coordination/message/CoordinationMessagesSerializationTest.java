package com.orientechnologies.orient.distriubted.context.coordination.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailPropose;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeFirstConnect;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.OSuccessPropose;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.topology.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.topology.OEnstablishTopology;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import com.orientechnologies.orient.distributed.db.ODropDbMessage;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
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

  private OStructuralMessage writeRead(OStructuralMessage message) throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);

    message.writeNetwork(out);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInput input = new DataInputStream(inputStream);
    return OStructuralMessage.readNetwork(input);
  }

  @Test
  public void proposeDrop() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    OProposeOp propose = new OProposeOp(id, new ODropDbMessage("db-name"));

    OProposeOp read = (OProposeOp) writeRead(propose);

    assertEquals(read.getPromiseId(), id);
    OOperationMessage operation = read.getOp();

    assertEquals(((ODropDbMessage) operation).getName(), "db-name");
  }

  @Test
  public void proposeEnstablish() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    var node1 = newNodeId();
    var node2 = newNodeId();
    Set<ONodeId> nodes = new HashSet<>();
    nodes.add(node1);
    nodes.add(node2);
    OProposeOp propose = new OProposeOp(id, new OEnstablishTopology(nodes));

    OProposeOp read = (OProposeOp) writeRead(propose);

    assertEquals(read.getPromiseId(), id);
    OOperationMessage operation = read.getOp();

    assertTrue(((OEnstablishTopology) operation).getCandidates().contains(node1));
    assertTrue(((OEnstablishTopology) operation).getCandidates().contains(node2));
  }

  @Test
  public void proposeAddNode() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    var node1 = newNodeId();
    OProposeOp propose = new OProposeOp(id, new OAddTopologyMember(1, node1));

    OProposeOp read = (OProposeOp) writeRead(propose);

    assertEquals(read.getPromiseId(), id);
    OOperationMessage operation = read.getOp();

    assertEquals(((OAddTopologyMember) operation).getVersion(), 1);
    assertEquals(((OAddTopologyMember) operation).getNode(), node1);
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
    OFailPropose succ = new OFailPropose(reply, id, new OInvalidSequential());

    OFailPropose read = (OFailPropose) writeRead(succ);

    assertEquals(read.getPromise(), id);
    assertEquals(read.getNodeId(), reply);
    assertTrue(read.getAcceptResult() instanceof OInvalidSequential);
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

  @Test
  public void firstConnectTest() throws IOException {

    ONodeId nodeId = newNodeId();
    ONodeStateNetwork net =
        new ONodeStateNetwork(Optional.empty(), OTopologyState.BOOT, new HashSet<>(), 0);
    ONodeFirstConnect succ = new ONodeFirstConnect(nodeId, net);

    ONodeFirstConnect read = (ONodeFirstConnect) writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    assertEquals(read.getState().getState(), OTopologyState.BOOT);
    assertEquals(read.getState().getVersion(), 0);
    assertTrue(read.getState().getNetworkId().isEmpty());
    assertTrue(read.getState().getMembers().isEmpty());
    Set<ONodeId> nodes = Set.of(newNodeId(), newNodeId());
    net = new ONodeStateNetwork(Optional.of("examplenetid"), OTopologyState.ESTABLISHED, nodes, 10);
    succ = new ONodeFirstConnect(nodeId, net);

    read = (ONodeFirstConnect) writeRead(succ);

    assertEquals(read.getNodeId(), nodeId);
    assertEquals(read.getState().getState(), OTopologyState.ESTABLISHED);
    assertEquals(read.getState().getVersion(), 10);
    assertEquals(read.getState().getMembers(), nodes);
    assertEquals(read.getState().getNetworkId(), Optional.of("examplenetid"));
  }
}
