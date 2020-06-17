package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.ODatabaseLeaderElected;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkOperationTest {

  @Test
  public void testSerialize() throws IOException {
    String db = "testdb";
    ONodeIdentity leader = new ONodeIdentity("foo", "bar");
    OLogId oplogId = new OLogId(10, 20, 19);
    ODatabaseLeaderElected op = new ODatabaseLeaderElected(db, leader, Optional.of(oplogId));
    ONetworkOperation operation = new ONetworkOperation(op);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    operation.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkOperation operation2 = new ONetworkOperation();
    operation2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(operation.getCommand(), operation2.getCommand());
    ODatabaseLeaderElected op2 = (ODatabaseLeaderElected) operation2.getOperation();
    Assert.assertEquals(op.getOperationId(), op2.getOperationId());
    Assert.assertEquals(op.getDatabase(), op2.getDatabase());
    Assert.assertEquals(op.getId(), op2.getId());
    Assert.assertEquals(op.getLeader(), op2.getLeader());
  }
}
