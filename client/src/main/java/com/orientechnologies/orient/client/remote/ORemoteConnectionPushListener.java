package com.orientechnologies.orient.client.remote;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelListener;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

/**
 * Created by tglman on 01/10/15.
 */
public class ORemoteConnectionPushListener implements ORemoteServerEventListener {

  private Set<ORemoteServerEventListener>                                            listeners = Collections
      .synchronizedSet(Collections.newSetFromMap(new WeakHashMap<ORemoteServerEventListener, Boolean>()));
  private ConcurrentMap<ORemoteServerEventListener, Set<OChannelBinaryAsynchClient>> conns     = new ConcurrentHashMap<ORemoteServerEventListener, Set<OChannelBinaryAsynchClient>>();

  public ORemoteConnectionPushListener(){
  }
  
  public void addListener(final ORemoteConnectionPool pool, final OChannelBinaryAsynchClient connection, final OStorageRemoteAsynchEventListener listener) {
    this.listeners.add(listener);
    Set<OChannelBinaryAsynchClient> ans = conns.get(listener);
    if (ans == null) {
      ans =Collections.synchronizedSet(new HashSet<OChannelBinaryAsynchClient>());
      Set<OChannelBinaryAsynchClient> putRet = conns.putIfAbsent(listener, ans);
      if(putRet != null)
        ans = putRet;
    }
    if (!ans.contains(connection)) {
      ans.add(connection);
      connection.registerListener(new OChannelListener() {
        @Override
        public void onChannelClose(OChannel iChannel) {
          Set<OChannelBinaryAsynchClient> all = conns.get(listener);
          all.remove(iChannel);
          if (all.isEmpty()){
            listener.onEndUsedConnections(pool);
          }
          connection.unregisterListener(this);
        }
      });
    }
  }

  public void removeListener(ORemoteServerEventListener listener) {
    this.listeners.remove(listener);
  }

  public void onRequest(final byte iRequestCode, Object obj) {
    for (ORemoteServerEventListener listener : listeners) {
      listener.onRequest(iRequestCode, obj);
    }
  }
}
