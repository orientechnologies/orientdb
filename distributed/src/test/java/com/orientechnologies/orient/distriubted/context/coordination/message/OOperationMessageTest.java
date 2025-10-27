package com.orientechnologies.orient.distriubted.context.coordination.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEnstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseState;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class OOperationMessageTest {

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

  private <T extends OOperationMessage> T writeRead(T message) throws IOException {
    OTransactionIdPromise id = newPromiseId();
    OProposeOp propose = new OProposeOp(id, message);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);

    propose.writeNetwork(out);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInput input = new DataInputStream(inputStream);
    OProposeOp read = ((OProposeOp) OStructuralMessage.readNetwork(input));

    assertEquals(read.getPromiseId(), id);
    return (T) read.getOp();
  }

  @Test
  public void proposeDrop() throws IOException {
    ODropDbMessage read = writeRead(new ODropDbMessage("db-name"));
    assertEquals(read.getName(), "db-name");
  }

  @Test
  public void proposeEnstablish() throws IOException {

    var node1 = newNodeId();
    var node2 = newNodeId();
    var groupId = newGroupId();
    Set<ONodeId> nodes = new HashSet<>();
    nodes.add(node1);
    nodes.add(node2);

    OEnstablishTopology toTest = new OEnstablishTopology(groupId, nodes);
    OEnstablishTopology operation = writeRead(toTest);

    assertTrue(operation.getCandidates().contains(node1));
    assertTrue(operation.getCandidates().contains(node2));
    assertTrue(operation.getGroupId().equals(groupId));
  }

  @Test
  public void setDatabaseSetState() throws IOException {

    ODatabaseId dbId = newDatabaseId();
    ONodeId nodeId1 = newNodeId();

    OSetDatabaseState toTest = new OSetDatabaseState(dbId, nodeId1, ODatabaseState.Online, 1);
    OSetDatabaseState operation = writeRead(toTest);

    assertEquals(operation.getDbId(), dbId);
    assertEquals(operation.getNodeId(), nodeId1);
    assertEquals(operation.getState(), ODatabaseState.Online);
    assertEquals(operation.getVersion(), 1);
  }

  @Test
  public void declareDatabase() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    ODatabaseId dbId = newDatabaseId();
    ONodeId nodeId1 = newNodeId();
    ONodeId nodeId2 = newNodeId();
    Set<ONodeId> paratecipants = new HashSet<>();
    paratecipants.add(id.getCoordinator());
    paratecipants.add(nodeId1);
    paratecipants.add(nodeId2);
    ODeclareDbMessage toTest = new ODeclareDbMessage("dbName", dbId, paratecipants, 2);

    ODeclareDbMessage operation = writeRead(toTest);

    assertEquals(operation.getName(), "dbName");
    assertEquals(operation.getId(), dbId);
    assertEquals(operation.getPartecipants(), paratecipants);
    assertEquals(operation.getMinimumQuorum(), 2);
  }

  @Test
  public void proposeAddNode() throws IOException {

    var node1 = newNodeId();
    OAddTopologyMember toTest = new OAddTopologyMember(1, node1);

    OAddTopologyMember operation = writeRead(toTest);

    assertEquals(operation.getVersion(), 1);
    assertEquals(operation.getNode(), node1);
  }
}
