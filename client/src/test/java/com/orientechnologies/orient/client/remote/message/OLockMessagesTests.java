package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.IOException;
import org.junit.Test;

public class OLockMessagesTests {

  @Test
  public void testReadWriteLockRequest() throws IOException {
    OLockRecordRequest request =
        new OLockRecordRequest(new ORecordId(10, 10), OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK, 10);
    MockChannel channel = new MockChannel();
    request.write(channel, null);

    channel.close();

    OLockRecordRequest other = new OLockRecordRequest();
    other.read(channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertEquals(other.getIdentity(), request.getIdentity());
    assertEquals(other.getTimeout(), request.getTimeout());
    assertEquals(other.getLockingStrategy(), request.getLockingStrategy());
  }

  @Test
  public void testReadWriteLockResponse() throws IOException {
    OLockRecordResponse response = new OLockRecordResponse((byte) 1, 2, "value".getBytes());
    MockChannel channel = new MockChannel();
    response.write(channel, 0, null);

    channel.close();

    OLockRecordResponse other = new OLockRecordResponse();
    other.read(channel, null);
    assertEquals(other.getRecordType(), response.getRecordType());
    assertEquals(other.getVersion(), response.getVersion());
    assertArrayEquals(other.getRecord(), response.getRecord());
  }

  @Test
  public void testReadWriteUnlockRequest() throws IOException {
    OUnlockRecordRequest request = new OUnlockRecordRequest(new ORecordId(10, 10));
    MockChannel channel = new MockChannel();
    request.write(channel, null);

    channel.close();

    OUnlockRecordRequest other = new OUnlockRecordRequest();
    other.read(channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());
    assertEquals(other.getIdentity(), request.getIdentity());
  }
}
