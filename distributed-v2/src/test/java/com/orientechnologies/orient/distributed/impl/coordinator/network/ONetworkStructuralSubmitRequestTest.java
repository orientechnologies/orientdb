package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.submit.ODropDatabaseSubmitRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkStructuralSubmitRequestTest {

  @Test
  public void testSerialize() throws IOException {

    OSessionOperationId opId = new OSessionOperationId("foo");
    ODropDatabaseSubmitRequest req = new ODropDatabaseSubmitRequest("bar");
    ONetworkStructuralSubmitRequest request = new ONetworkStructuralSubmitRequest(opId, req);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    request.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkStructuralSubmitRequest request2 = new ONetworkStructuralSubmitRequest();
    request2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(request.getCommand(), request2.getCommand());
    Assert.assertEquals(request.getOperationId(), request2.getOperationId());
    ODropDatabaseSubmitRequest req2 = (ODropDatabaseSubmitRequest) request2.getRequest();
    Assert.assertEquals(req.getRequestType(), req2.getRequestType());
    Assert.assertEquals(req.getClass(), req2.getClass());
    Assert.assertEquals(req.getDatabase(), req2.getDatabase());
  }
}
