package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Created by tglman on 09/06/16.
 */
public class OStorageRemoteAsyncOperationTest {

  private OStorageRemote             storage;

  @Mock
  private OChannelBinaryAsynchClient channel;

  @Mock
  private ORemoteConnectionManager   connectionManager;
  @Mock
  private OStorageRemoteSession      session;
  @Mock
  private OStorageRemoteNodeSession  nodeSession;

  private class CallStatus {
    public String status;
  }

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(session.getServerSession(Mockito.anyString())).thenReturn(nodeSession);
    storage = new OStorageRemote("mock", "mock", "mock") {
      @Override
      public <T> T baseNetworkOperation(OStorageRemoteOperation<T> operation, String errorMessage, int retry) {
        try {
          return operation.execute(channel, session);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    };
    storage.connectionManager = connectionManager;
  }

  @Test
  public void testSyncCall() {
    final CallStatus status = new CallStatus();
    storage.asyncNetworkOperation(new OBinaryAsyncRequest<OBinaryResponse>() {
      @Override
      public byte getCommand() {
        return 0;
      }

      @Override
      public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        assertNull(status.status);
        status.status = "write";
      }

      @Override
      public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {

      }

      @Override
      public byte getMode() {
        return 0;
      }

      @Override
      public void setMode(byte mode) {
      }

      @Override
      public String getDescription() {
        return null;
      }

      @Override
      public OBinaryResponse execute(OBinaryRequestExecutor executor) {
        return null;
      }

      @Override
      public OBinaryResponse createResponse() {
        return new OBinaryResponse() {
          @Override
          public void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
            assertEquals(status.status, "write");
            status.status = "read";
          }

          @Override
          public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
          }

        };
      }
    }, 0, new ORecordId(-1, -1), null, "");

    assertEquals(status.status, "read");
  }

  @Test
  public void testNoReadCall() {
    final CallStatus status = new CallStatus();
    storage.asyncNetworkOperation(new OBinaryAsyncRequest<OBinaryResponse>() {
      @Override
      public byte getCommand() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        assertNull(status.status);
        status.status = "write";
      }

      @Override
      public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
      }

      @Override
      public byte getMode() {
        return 0;
      }

      @Override
      public void setMode(byte mode) {

      }

      @Override
      public String getDescription() {
        return null;
      }

      @Override
      public OBinaryResponse execute(OBinaryRequestExecutor executor) {
        return null;
      }

      @Override
      public OBinaryResponse createResponse() {

        return new OBinaryResponse() {
          @Override
          public void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
            fail();
          }

          @Override
          public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
          }
        };
      }

    }, 1, new ORecordId(-1, -1), null, "");

    assertEquals(status.status, "write");
  }

  @Test
  public void testAsyncRead() throws InterruptedException {
    final CallStatus status = new CallStatus();
    final CountDownLatch callBackWait = new CountDownLatch(1);
    final CountDownLatch readDone = new CountDownLatch(1);
    final CountDownLatch callBackDone = new CountDownLatch(1);
    storage.asyncNetworkOperation(new OBinaryAsyncRequest<OBinaryResponse>() {
      @Override
      public byte getCommand() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        assertNull(status.status);
        status.status = "write";
      }

      @Override
      public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {

      }

      @Override
      public byte getMode() {
        return 0;
      }

      @Override
      public void setMode(byte mode) {
      }

      @Override
      public String getDescription() {
        return null;
      }

      @Override
      public OBinaryResponse execute(OBinaryRequestExecutor executor) {
        return null;
      }

      @Override
      public OBinaryResponse createResponse() {
        return new OBinaryResponse() {
          @Override
          public void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
            try {
              if (callBackWait.await(10, TimeUnit.MILLISECONDS))
                readDone.countDown();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
          }
        };
      }

    }, 1, new ORecordId(-1, -1), new ORecordCallback<OBinaryResponse>() {
      @Override
      public void call(ORecordId iRID, OBinaryResponse iParameter) {
        callBackDone.countDown();
      }
    }, "");

    // SBLCK THE CALLBAC THAT SHOULD BE IN ANOTHER THREAD
    callBackWait.countDown();

    boolean called = readDone.await(10, TimeUnit.MILLISECONDS);
    if (!called)
      fail("Read not called");
    called = callBackDone.await(10, TimeUnit.MILLISECONDS);
    if (!called)
      fail("Callback not called");
  }

}
