package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkSubmitRequestTest {

  @Test
  public void testSerialize() throws IOException {

    String db = "testDb";
    OSessionOperationId opId = new OSessionOperationId("foo");
    ODDLQuerySubmitRequest req = new ODDLQuerySubmitRequest("create class FooBar");
    ONetworkSubmitRequest request = new ONetworkSubmitRequest(db, opId, req);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    request.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkSubmitRequest request2 = new ONetworkSubmitRequest();
    request2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(request.getCommand(), request2.getCommand());
    Assert.assertEquals(request.getDatabase(), request2.getDatabase());
    Assert.assertEquals(request.getOperationId(), request2.getOperationId());
    OSubmitRequest req2 = request2.getRequest();
    Assert.assertEquals(req.getRequestType(), req2.getRequestType());
    Assert.assertEquals(req.getClass(), req2.getClass());
    Assert.assertEquals(req.getQuery(), ((ODDLQuerySubmitRequest) req2).getQuery());
  }
}
