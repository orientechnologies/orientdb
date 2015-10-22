package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by tglman on 22/10/15.
 */
public class ORemoteConnectionPushListenerTest {

  private int                        callCount = 0;
  private ORemoteServerEventListener listener  = new ORemoteServerEventListener() {
    @Override
    public void onRequest(byte iRequestCode, Object obj) {
      callCount++;
    }

    @Override
    public void registerLiveListener(Integer id, OLiveResultListener listener) {

    }

    @Override
    public void unregisterLiveListener(Integer id) {

    }
  };

  @Test
  public void testConnectionPoolListenerPropagate() {

    ORemoteConnectionPushListener pool = new ORemoteConnectionPushListener();
    callCount = 0;
    pool.addListener(listener);
    pool.onRequest((byte) 10, null);
    assertEquals(callCount, 1);
  }

  @Test
  public void testRegistredOnlyOnce() {

    ORemoteConnectionPushListener pool = new ORemoteConnectionPushListener();
    callCount = 0;
    pool.addListener(listener);
    pool.addListener(listener);
    pool.onRequest((byte) 10, null);
    assertEquals(callCount, 1);
  }


}
