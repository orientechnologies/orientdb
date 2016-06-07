package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.ODatabaseSessionMetadata;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.util.*;

/**
 * Created by tglman on 31/03/16.
 */
public class OStorageRemoteSession implements ODatabaseSessionMetadata {
  public boolean                                commandExecuting       = false;
  public int                                    serverURLIndex         = -1;
  public Set<OChannelBinary>                    connections            = Collections.newSetFromMap( new WeakHashMap<OChannelBinary, Boolean>());
  public String                                 connectionUserName     = null;
  public String                                 connectionUserPassword = null;
  public Map<String, OStorageRemoteNodeSession> sessions               = new HashMap<String, OStorageRemoteNodeSession>();

  public final Integer uniqueClientSessionId;

  public OStorageRemoteSession(int sessionId) {
    this.uniqueClientSessionId = sessionId;
  }

  public boolean has(final OChannelBinary connection) {
    return connections.contains(connection);
  }

  public OStorageRemoteNodeSession get(String serverURL) {
    return sessions.get(serverURL);
  }

  public OStorageRemoteNodeSession getOrCreate(String serverURL) {
    OStorageRemoteNodeSession session = sessions.get(serverURL);
    if (session == null) {
      session = new OStorageRemoteNodeSession(serverURL, uniqueClientSessionId);
      sessions.put(serverURL, session);
    }
    return session;
  }

  public void add(final OChannelBinary connection) {
    connections.add(connection);
  }

  public void clear() {
    connections.clear();
  }

  public void close() {
    commandExecuting = false;
    serverURLIndex = -1;
    connections = new HashSet<OChannelBinary>();
    sessions = new HashMap<String, OStorageRemoteNodeSession>();
  }

  public Integer getSessionId() {
    if (sessions.isEmpty())
      return -1;
    OStorageRemoteNodeSession curSession = sessions.values().iterator().next();
    return curSession.getSessionId();
  }

  public String getServerUrl() {
    if (sessions.isEmpty())
      return null;
    OStorageRemoteNodeSession curSession = sessions.values().iterator().next();
    return curSession.getServerURL();
  }

  public void remove(String serverURL) {
    sessions.remove(serverURL);
  }

  public Collection<OStorageRemoteNodeSession> getAll() {
    return sessions.values();
  }
}
