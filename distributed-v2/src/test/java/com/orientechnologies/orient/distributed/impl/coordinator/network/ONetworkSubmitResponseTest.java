package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkSubmitResponseTest {

  @Test
  public void testSerialize() throws IOException {

    String db = "testDb";
    OSessionOperationId opId = new OSessionOperationId("foo");
    ODDLQuerySubmitResponse res = new ODDLQuerySubmitResponse();
    ONetworkSubmitResponse response = new ONetworkSubmitResponse(db, opId, res);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    response.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkSubmitResponse response2 = new ONetworkSubmitResponse();
    response2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(response.getCommand(), response2.getCommand());
    Assert.assertEquals(response.getDatabase(), response2.getDatabase());
    Assert.assertEquals(response.getOperationId(), response2.getOperationId());
    OSubmitResponse res2 = response2.getResponse();
    Assert.assertEquals(res.getResponseType(), res2.getResponseType());
    Assert.assertEquals(res.getClass(), res2.getClass());
  }
}
