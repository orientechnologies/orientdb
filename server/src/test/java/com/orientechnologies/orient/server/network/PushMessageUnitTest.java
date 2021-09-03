package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.OStorageRemotePushThread;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Created by tglman on 10/05/17. */
public class PushMessageUnitTest {

  public class MockPushResponse implements OBinaryPushResponse {

    @Override
    public void write(OChannelDataOutput network) throws IOException {}

    @Override
    public void read(OChannelDataInput channel) throws IOException {
      responseRead.countDown();
    }
  }

  public class MockPushRequest implements OBinaryPushRequest<OBinaryPushResponse> {
    @Override
    public void write(OChannelDataOutput channel) throws IOException {
      requestWritten.countDown();
    }

    @Override
    public byte getPushCommand() {
      return 100;
    }

    @Override
    public void read(OChannelDataInput network) throws IOException {}

    @Override
    public OBinaryPushResponse execute(ORemotePushHandler remote) {
      executed.countDown();
      return new MockPushResponse();
    }

    @Override
    public OBinaryPushResponse createResponse() {
      return new MockPushResponse();
    }
  }

  public class MockPushRequestNoResponse implements OBinaryPushRequest<OBinaryPushResponse> {
    @Override
    public void write(OChannelDataOutput channel) throws IOException {
      requestWritten.countDown();
    }

    @Override
    public byte getPushCommand() {
      return 101;
    }

    @Override
    public void read(OChannelDataInput network) throws IOException {}

    @Override
    public OBinaryPushResponse execute(ORemotePushHandler remote) {
      executed.countDown();
      return null;
    }

    @Override
    public OBinaryPushResponse createResponse() {
      return null;
    }
  }

  private CountDownLatch requestWritten;
  private CountDownLatch responseRead;
  private CountDownLatch executed;
  private MockPipeChannel channelBinaryServer;
  private MockPipeChannel channelBinaryClient;
  @Mock private OServer server;

  @Mock private ORemotePushHandler remote;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    PipedInputStream inputClient = new PipedInputStream();
    PipedOutputStream outputServer = new PipedOutputStream(inputClient);
    PipedInputStream inputServer = new PipedInputStream();
    PipedOutputStream outputClient = new PipedOutputStream(inputServer);
    this.channelBinaryClient = new MockPipeChannel(inputClient, outputClient);
    this.channelBinaryServer = new MockPipeChannel(inputServer, outputServer);
    Mockito.when(server.getContextConfiguration()).thenReturn(new OContextConfiguration());
    Mockito.when(remote.getNetwork(Mockito.anyString())).thenReturn(channelBinaryClient);
    Mockito.when(remote.createPush((byte) 100)).thenReturn(new MockPushRequest());
    Mockito.when(remote.createPush((byte) 101)).thenReturn(new MockPushRequestNoResponse());
    requestWritten = new CountDownLatch(1);
    responseRead = new CountDownLatch(1);
    executed = new CountDownLatch(1);
  }

  @Test
  public void testPushMessage() throws IOException, InterruptedException {
    ONetworkProtocolBinary binary = new ONetworkProtocolBinary(server);
    binary.initVariables(server, channelBinaryServer);
    new Thread(
            () -> {
              try {
                binary.push(new MockPushRequest());
              } catch (IOException e) {
                e.printStackTrace();
              }
            })
        .start();
    binary.start();
    assertTrue(requestWritten.await(10, TimeUnit.SECONDS));
    OStorageRemotePushThread pushThread = new OStorageRemotePushThread(remote, "none", 10, 1000);
    pushThread.start();

    assertTrue(executed.await(10, TimeUnit.SECONDS));
    assertTrue(responseRead.await(10, TimeUnit.SECONDS));
    Mockito.verify(remote).createPush((byte) 100);
    pushThread.shutdown();
    binary.shutdown();
  }

  @Test
  public void testPushMessageNoResponse() throws IOException, InterruptedException {
    ONetworkProtocolBinary binary = new ONetworkProtocolBinary(server);
    binary.initVariables(server, channelBinaryServer);
    Thread thread =
        new Thread(
            () -> {
              try {
                assertNull(binary.push(new MockPushRequestNoResponse()));
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
    thread.start();
    binary.start();
    assertTrue(requestWritten.await(10, TimeUnit.SECONDS));
    OStorageRemotePushThread pushThread = new OStorageRemotePushThread(remote, "none", 10, 1000);
    pushThread.start();

    assertTrue(executed.await(10, TimeUnit.SECONDS));
    Mockito.verify(remote).createPush((byte) 101);
    thread.join(1000);
    pushThread.shutdown();
    pushThread.join(1000);
    binary.shutdown();
  }
}
