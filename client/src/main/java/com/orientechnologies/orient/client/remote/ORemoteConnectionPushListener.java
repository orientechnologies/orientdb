package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by tglman on 01/10/15.
 */
public class ORemoteConnectionPushListener implements ORemoteServerEventListener {

  private Set<ORemoteServerEventListener> listeners = Collections.synchronizedSet(new HashSet<ORemoteServerEventListener>());

  public void addListener(ORemoteServerEventListener listener) {
    this.listeners.add(listener);
  }

  public void removeListener(ORemoteServerEventListener listener) {
    this.listeners.remove(listener);
  }

  public void onRequest(final byte iRequestCode, Object obj) {
    for (ORemoteServerEventListener listener : listeners) {
      listener.onRequest(iRequestCode, obj);
    }
  }

  @Override
  public void registerLiveListener(Integer id, OLiveResultListener listener) {

  }

  @Override
  public void unregisterLiveListener(Integer id) {

  }


}
