package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkConfirmTest {

  @Test
  public void testSerialize() throws IOException {
    OLogId oplogId = new OLogId(10, 20, 19);
    ONetworkConfirm ack = new ONetworkConfirm(oplogId);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    ack.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkConfirm ack2 = new ONetworkConfirm();
    ack2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(ack.getCommand(), ack2.getCommand());
    Assert.assertEquals(ack.getId(), ack2.getId());
  }
}
