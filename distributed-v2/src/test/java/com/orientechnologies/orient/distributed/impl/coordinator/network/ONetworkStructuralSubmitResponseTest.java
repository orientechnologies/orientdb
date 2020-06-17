package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.submit.ODropDatabaseSubmitResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ONetworkStructuralSubmitResponseTest {

  @Test
  public void testSerialize1() throws IOException {

    OSessionOperationId opId = new OSessionOperationId("foo");
    ODropDatabaseSubmitResponse res = new ODropDatabaseSubmitResponse(true, null);
    ONetworkStructuralSubmitResponse response = new ONetworkStructuralSubmitResponse(opId, res);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    response.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkStructuralSubmitResponse response2 = new ONetworkStructuralSubmitResponse();
    response2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(response.getCommand(), response2.getCommand());
    Assert.assertEquals(response.getOperationId(), response2.getOperationId());
    ODropDatabaseSubmitResponse res2 = (ODropDatabaseSubmitResponse) response2.getResponse();
    Assert.assertEquals(res.getResponseType(), res2.getResponseType());
    Assert.assertEquals(res.getClass(), res2.getClass());
    Assert.assertEquals(res.getError(), res2.getError());
    Assert.assertEquals(res.isSuccess(), res2.isSuccess());
  }

  @Test
  public void testSerialize2() throws IOException {

    OSessionOperationId opId = new OSessionOperationId("foo");
    ODropDatabaseSubmitResponse res = new ODropDatabaseSubmitResponse(false, "an error!");
    ONetworkStructuralSubmitResponse response = new ONetworkStructuralSubmitResponse(opId, res);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    response.write(out);
    byte[] serialized = baos.toByteArray();
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(bais);

    ONetworkStructuralSubmitResponse response2 = new ONetworkStructuralSubmitResponse();
    response2.read(in);
    in.close();
    bais.close();

    Assert.assertEquals(response.getCommand(), response2.getCommand());
    Assert.assertEquals(response.getOperationId(), response2.getOperationId());
    ODropDatabaseSubmitResponse res2 = (ODropDatabaseSubmitResponse) response2.getResponse();
    Assert.assertEquals(res.getResponseType(), res2.getResponseType());
    Assert.assertEquals(res.getClass(), res2.getClass());
    Assert.assertEquals(res.getError(), res2.getError());
    Assert.assertEquals(res.isSuccess(), res2.isSuccess());
  }
}
