package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.raft.ONodeJoin;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkPropagateTest {

  @Test
  public void testSerialize() throws IOException {

    ONodeIdentity leader = new ONodeIdentity("foo", "bar");
    ONodeJoin join = new ONodeJoin(leader);
    OLogId oplogId = new OLogId(10, 20, 19);
    ONetworkPropagate operation = new ONetworkPropagate(oplogId, join);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    operation.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkPropagate operation2 = new ONetworkPropagate();
    operation2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(operation.getCommand(), operation2.getCommand());
    Assert.assertEquals(operation.getId(), operation2.getId());
    ONodeJoin join2 = (ONodeJoin) operation2.getOperation();
    Assert.assertEquals(join.getRequestType(), join2.getRequestType());
    Assert.assertEquals(join.getRequesterSequential(), join2.getRequesterSequential());
  }
}
