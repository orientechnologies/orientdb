package com.orientechnologies.orient.distriubted.context.coordination.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddDatabaseMembers;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
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
import java.util.List;
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
    var version = new OVersion(0);
    var dbId = newDatabaseId();
    ODropDbMessage read = writeRead(new ODropDbMessage(dbId, version));
    assertEquals(read.getDbId(), dbId);
    assertEquals(read.getVersion(), version);
  }

  @Test
  public void proposeEstablish() throws IOException {

    var node1 = newNodeId();
    var node2 = newNodeId();
    var groupId = newGroupId();
    Set<ONodeId> nodes = new HashSet<>();
    nodes.add(node1);
    nodes.add(node2);

    OEstablishTopology toTest = new OEstablishTopology(groupId, nodes);
    OEstablishTopology operation = writeRead(toTest);

    assertTrue(operation.getCandidates().contains(node1));
    assertTrue(operation.getCandidates().contains(node2));
    assertTrue(operation.getGroupId().equals(groupId));
  }

  @Test
  public void setDatabaseSetState() throws IOException {

    ODatabaseId dbId = newDatabaseId();
    ONodeId nodeId1 = newNodeId();

    OSetDatabaseState toTest =
        new OSetDatabaseState(dbId, nodeId1, ODatabaseState.Online, new OVersion(1));
    OSetDatabaseState operation = writeRead(toTest);

    assertEquals(operation.getDbId(), dbId);
    assertEquals(operation.getNodeId(), nodeId1);
    assertEquals(operation.getState(), ODatabaseState.Online);
    assertEquals(operation.getVersion(), new OVersion(1));
  }

  @Test
  public void declareDatabase() throws IOException {

    OTransactionIdPromise id = newPromiseId();
    ODatabaseId dbId = newDatabaseId();
    ONodeId nodeId1 = newNodeId();
    ONodeId nodeId2 = newNodeId();
    Set<OAddNodeInfo> paratecipants = new HashSet<>();
    paratecipants.add(new OAddNodeInfo(id.getCoordinator(), ONodeRole.Main));
    paratecipants.add(new OAddNodeInfo(nodeId1, ONodeRole.Main));
    paratecipants.add(new OAddNodeInfo(nodeId2, ONodeRole.Main));
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
    OAddTopologyMember toTest = new OAddTopologyMember(new OVersion(1), node1);

    OAddTopologyMember operation = writeRead(toTest);

    assertEquals(operation.getVersion().getValue(), 1);
    assertEquals(operation.getNode(), node1);
  }

  @Test
  public void proposeAddMember() throws IOException {

    var node1 = newNodeId();
    var dbId = newDatabaseId();
    var nodes = List.of(new OAddNodeInfo(node1, ONodeRole.Main));
    OAddDatabaseMembers toTest = new OAddDatabaseMembers(new OVersion(1), dbId, nodes);

    OAddDatabaseMembers operation = writeRead(toTest);

    assertEquals(operation.getVersion().getValue(), 1);
    assertEquals(operation.getDbId(), dbId);
    assertEquals(operation.getNodes().get(0).node(), node1);
    assertEquals(operation.getNodes().get(0).role(), ONodeRole.Main);
  }
}
