package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemotePushThread;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 10/05/17.
 */
public class PushMessageUnitTest {

  public class MockPushResponse implements OBinaryPushResponse {

    @Override
    public void write(OChannelDataOutput network) throws IOException {

    }

    @Override
    public void read(OChannelDataInput channel) throws IOException {
      responseRead.countDown();
      System.out.println("response read");
    }
  }

  public class MockPushRequest implements OBinaryPushRequest<OBinaryPushResponse> {
    @Override
    public void write(OChannelDataOutput channel) throws IOException {
      System.out.println("written");
      requestWritten.countDown();
    }

    @Override
    public byte getPushCommand() {
      return 100;
    }

    @Override
    public void read(OChannelDataInput network) throws IOException {

    }

    @Override
    public OBinaryPushResponse execute(ORemotePushHandler remote) {
      executed.countDown();
      System.out.println("executed");
      return new MockPushResponse();
    }

    @Override
    public OBinaryPushResponse createResponse() {
      return new MockPushResponse();
    }
  }

  private CountDownLatch requestWritten = new CountDownLatch(1);
  private CountDownLatch responseRead   = new CountDownLatch(1);
  private MockPipeChannel channelBinaryServer;
  private MockPipeChannel channelBinaryClient;

  @Mock
  private OServer server;

  @Mock
  private ORemotePushHandler remote;

  private CountDownLatch executed = new CountDownLatch(1);

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
    Mockito.when(remote.createPush(Mockito.anyByte())).thenReturn(new MockPushRequest());
  }

  @Test
  public void testPushMessage() throws IOException, InterruptedException {
    ONetworkProtocolBinary binary = new ONetworkProtocolBinary(server);
    binary.initVariables(server, channelBinaryServer);
    new Thread(() -> {
      try {
        binary.push(new MockPushRequest());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }).start();
    binary.start();
    assertTrue(requestWritten.await(10, TimeUnit.SECONDS));
    OStorageRemotePushThread pushThread = new OStorageRemotePushThread(remote, "none");
    pushThread.start();

    assertTrue(executed.await(10, TimeUnit.SECONDS));
    assertTrue(responseRead.await(10, TimeUnit.SECONDS));
    Mockito.verify(remote).createPush((byte) 100);
    pushThread.shutdown();
    binary.shutdown();

  }

}
