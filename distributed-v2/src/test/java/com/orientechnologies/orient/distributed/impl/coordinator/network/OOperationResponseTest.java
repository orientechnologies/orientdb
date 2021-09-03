package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class OOperationResponseTest {

  @Test
  public void testSerialize() throws IOException {

    String db = "testDb";
    OLogId oplogId = new OLogId(10, 20, 19);
    ODDLQueryOperationResponse res = new ODDLQueryOperationResponse();
    OOperationResponse response = new OOperationResponse(db, oplogId, res);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    response.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    OOperationResponse response2 = new OOperationResponse();
    response2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(response.getCommand(), response2.getCommand());
    Assert.assertEquals(response.getDatabase(), response2.getDatabase());
    Assert.assertEquals(response.getId(), response2.getId());
    ONodeResponse res2 = response2.getResponse();
    Assert.assertEquals(res.getResponseType(), res2.getResponseType());
    Assert.assertEquals(res.getClass(), res2.getClass());
  }
}
