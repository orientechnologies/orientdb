package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationRequest;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class OOperationRequestTest {

  @Test
  public void testSerialize() throws IOException {

    String db = "testDb";
    OLogId oplogId = new OLogId(10, 20, 19);
    ODDLQueryOperationRequest req = new ODDLQueryOperationRequest("create class FooBar");
    OOperationRequest request = new OOperationRequest(db, oplogId, req);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    request.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    OOperationRequest request2 = new OOperationRequest();
    request2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(request.getCommand(), request2.getCommand());
    Assert.assertEquals(request.getDatabase(), request2.getDatabase());
    Assert.assertEquals(request.getId(), request2.getId());
    ONodeRequest req2 = request2.getRequest();
    Assert.assertEquals(req.getRequestType(), req2.getRequestType());
    Assert.assertEquals(req.getClass(), req2.getClass());
    Assert.assertEquals(req.getQuery(), ((ODDLQueryOperationRequest) req2).getQuery());
  }
}
