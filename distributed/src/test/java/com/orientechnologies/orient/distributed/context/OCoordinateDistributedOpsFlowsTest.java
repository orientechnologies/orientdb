package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ORemoveDatabaseMembers;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ORemoveTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseMemberRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseQuorum;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetTopologyQuorum;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class OCoordinateDistributedOpsFlowsTest {

  @Test
  public void testChangeRole() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    flow.execute(new OSetDatabaseMemberRole(dbId, node3, ONodeRole.Replica, version));

    for (var c : flow.getContexts().values()) {
      var role = c.getOps().getDatabaseTopology().getRole(dbId, node3);
      assertEquals(ONodeRole.Replica, role);
    }
  }

  @Test
  public void testRemoveDatabaseMember() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    var result = flow.execute(new ORemoveDatabaseMembers(dbId, List.of(node3), version));

    assertTrue(result.isEmpty());
    for (var c : flow.getContexts().values()) {
      var members = c.getOps().getDatabaseTopology().getMembers(dbId);
      assertFalse(members.contains(node3));
    }
  }

  @Test
  public void testRemoveTopologyMember() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    @SuppressWarnings("unused")
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();

    var version = flow.getContexts().get(node1).getOps().nextTopologyVersion();
    var result = flow.execute(new ORemoveTopologyMember(node3, version));

    assertTrue(result.isEmpty());
    for (var c : flow.getContexts().values()) {
      var members = c.getOps().getNetworkTopology().getMembers();
      assertFalse(members.contains(node3));
    }
  }

  @Test
  public void testSetDatabaseQuorum() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    var result = flow.execute(new OSetDatabaseQuorum(dbId, 3, version));

    assertTrue(result.isEmpty());
    for (var c : flow.getContexts().values()) {
      var quormu = c.getOps().getDatabaseTopology().getQuorum(dbId);
      assertEquals(3, quormu);
    }
  }

  @Test
  public void testSetTopologyQuorum() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextTopologyVersion();
    var result = flow.execute(new OSetTopologyQuorum(3, version));

    assertTrue(result.isEmpty());
    for (var c : flow.getContexts().values()) {
      var quormu = c.getOps().getNetworkTopology().getQuorum();
      assertEquals(3, quormu);
    }
  }

  @Test
  public void testSetDatabaseState() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    var result = flow.execute(new OSetDatabaseState(dbId, node3, ODatabaseState.Offline, version));

    assertTrue(result.isEmpty());
    for (var c : flow.getContexts().values()) {
      var state = c.getOps().getDatabaseTopology().getState(dbId, node3);
      assertEquals(ODatabaseState.Offline, state);
    }

    var version1 = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    var result1 = flow.execute(new OSetDatabaseState(dbId, node3, ODatabaseState.Online, version1));

    assertTrue(result1.isEmpty());
    for (var c : flow.getContexts().values()) {
      var state = c.getOps().getDatabaseTopology().getState(dbId, node3);
      assertEquals(ODatabaseState.Online, state);
    }
  }

  @Test
  public void testSetDatabaseOneFail() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    flow.executeConcurrently(
        new OSetDatabaseQuorum(dbId, 3, version), new OSetDatabaseQuorum(dbId, 1, version));

    for (var c : flow.getContexts().values()) {
      var quormu = c.getOps().getDatabaseTopology().getQuorum(dbId);
      assertEquals(3, quormu);
    }
  }

  @Test
  public void testSetDatabaseOneFailInvertedConfirm() {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    var networkNodes = Set.of(node1, node2, node3);
    var pertecipants = networkNodes.stream().map(OAddNodeInfo::main).collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    System.out.println("staaaaaarrrrrtttt");
    var version = flow.getContexts().get(node1).getOps().nextDatabaseVersion(dbId);
    flow.executeConcurrentlySecondConfirmFirst(
        new OSetDatabaseQuorum(dbId, 3, version), new OSetDatabaseQuorum(dbId, 1, version));

    for (var c : flow.getContexts().values()) {
      var quormu = c.getOps().getDatabaseTopology().getQuorum(dbId);
      assertEquals(3, quormu);
    }
  }
}
