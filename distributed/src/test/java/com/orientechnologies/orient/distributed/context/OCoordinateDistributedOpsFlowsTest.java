package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseMemberRole;
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
    var pertecipants =
        networkNodes.stream()
            .map((x) -> new OAddNodeInfo(x, ONodeRole.Main))
            .collect(Collectors.toSet());
    ODatabaseId dbId = new ODatabaseId("test");
    flow.execute(new ODeclareDbMessage("test", dbId, pertecipants, 2));

    var version =
        flow.getContexts().get(node1).getOps().getDatabaseTopology().getDatabaseVersion(dbId);
    flow.execute(new OSetDatabaseMemberRole(dbId, node3, ONodeRole.Replica, version.next()));

    for (var c : flow.getContexts().values()) {
      var role = c.getOps().getDatabaseTopology().getRole(dbId, node3);
      assertEquals(ONodeRole.Replica, role);
    }
  }
}
