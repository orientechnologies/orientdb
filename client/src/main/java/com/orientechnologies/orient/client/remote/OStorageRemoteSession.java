package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.ODatabaseSessionMetadata;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by tglman on 31/03/16.
 */
public class OStorageRemoteSession implements ODatabaseSessionMetadata {
  public boolean             commandExecuting       = false;
  public Integer             sessionId              = -1;
  public String              serverURL              = null;
  public int                 serverURLIndex         = -1;
  public Map<String, byte[]> tokens                 = new HashMap<String, byte[]>();
  public Set<OChannelBinary> connections            = new HashSet<OChannelBinary>();
  public String              connectionUserName     = null;
  public String              connectionUserPassword = null;

  public final Integer uniqueClientSessionId;

  public OStorageRemoteSession(int sessionId) {
    this.sessionId = sessionId;
    this.uniqueClientSessionId = sessionId;
  }

  public boolean has(final OChannelBinary connection) {
    return connections.contains(connection);
  }

  public void add(final OChannelBinary connection) {
    connections.add(connection);
  }

  public void clear() {
    connections.clear();
  }

  public void close() {
    commandExecuting = false;
    sessionId = uniqueClientSessionId;
    serverURL = null;
    serverURLIndex = -1;
    tokens = new HashMap<String, byte[]>();
    connections = new HashSet<OChannelBinary>();
  }
}
