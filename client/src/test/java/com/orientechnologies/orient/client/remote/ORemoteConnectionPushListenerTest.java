package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelListener;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by tglman on 22/10/15.
 */
public class ORemoteConnectionPushListenerTest {

  @Test
  public void testConnectionPoolListenerPropagate() {

    OChannelBinaryAsynchClient chann = Mockito.mock(OChannelBinaryAsynchClient.class);
    OStorageRemoteAsynchEventListener listener = Mockito.mock(OStorageRemoteAsynchEventListener.class);
    ORemoteConnectionPool pool = Mockito.mock(ORemoteConnectionPool.class);
    ORemoteConnectionPushListener poolListener = new ORemoteConnectionPushListener();
    poolListener.addListener(pool, chann, listener);
    poolListener.onRequest((byte) 10, null);

    Mockito.verify(listener, VerificationModeFactory.only()).onRequest(Mockito.anyByte(), Mockito.anyObject());
  }

  @Test
  public void testRegistredOnlyOnce() {
    OChannelBinaryAsynchClient chann = Mockito.mock(OChannelBinaryAsynchClient.class);
    OStorageRemoteAsynchEventListener listener = Mockito.mock(OStorageRemoteAsynchEventListener.class);
    ORemoteConnectionPushListener poolListener = new ORemoteConnectionPushListener();
    ORemoteConnectionPool pool = Mockito.mock(ORemoteConnectionPool.class);
    poolListener.addListener(pool, chann, listener);
    poolListener.addListener(pool, chann, listener);
    poolListener.onRequest((byte) 10, null);

    Mockito.verify(listener, VerificationModeFactory.only()).onRequest(Mockito.anyByte(), Mockito.anyObject());

  }

  @Test
  public void testCloseListerner() {
    OChannelBinaryAsynchClient chann = Mockito.mock(OChannelBinaryAsynchClient.class);
    OStorageRemoteAsynchEventListener listener = Mockito.mock(OStorageRemoteAsynchEventListener.class);
    ORemoteConnectionPool pool = Mockito.mock(ORemoteConnectionPool.class);
    ArgumentCaptor<OChannelListener> captor = ArgumentCaptor.forClass(OChannelListener.class);
    Mockito.doNothing().when(chann).registerListener(captor.capture());

    ORemoteConnectionPushListener poolListener = new ORemoteConnectionPushListener();
    poolListener.addListener(pool, chann, listener);
    poolListener.addListener(pool, chann, listener);
    captor.getValue().onChannelClose(chann);

    Mockito.verify(listener, VerificationModeFactory.only()).onEndUsedConnections(pool);

  }


}
