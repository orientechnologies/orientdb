package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OPushEventType {
  private final ConcurrentMap<String, OBinaryPushRequest<?>> databases = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, Set<OPushInfo>> listeners = new ConcurrentHashMap<>();

  public synchronized void send(
      String database, OBinaryPushRequest<?> request, OPushManager pushManager) {
    OBinaryPushRequest<?> prev = databases.put(database, request);
    if (prev == null) {
      pushManager.genericNotify(listeners, database, this);
    }
  }

  public synchronized OBinaryPushRequest<?> getRequest(String database) {
    return databases.remove(database);
  }

  public synchronized void subscribe(String database, OPushInfo protocol) {
    Set<OPushInfo> pushSockets = listeners.get(database);
    if (pushSockets == null) {
      pushSockets = Collections.newSetFromMap(new ConcurrentHashMap<>());
      listeners.put(database, pushSockets);
    }
    pushSockets.add(protocol);
  }

  public synchronized void cleanListeners(OPushManager manager) {
    for (Set<OPushInfo> value : listeners.values()) {
      Iterator<OPushInfo> iter = value.iterator();
      while (iter.hasNext()) {
        OPushInfo ref = iter.next();
        if (ref.protocol().get() == null) {
          manager.close(ref);
          iter.remove();
        }
      }
    }
  }
}
