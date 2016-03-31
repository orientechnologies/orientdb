package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by tglman on 31/03/16.
 */
public class OStorageRemoteSession {
  public boolean             commandExecuting = false;
  public Integer             sessionId        = -1;
  public String              serverURL        = null;
  public int                 serverURLIndex   = -1;
  public Set<OChannelBinary> connections      = new HashSet<OChannelBinary>();

  public boolean has(final OChannelBinary connection) {
    return connections.contains(connection);
  }

  public void add(final OChannelBinary connection) {
    connections.add(connection);
  }

  public void clear() {
    connections.clear();
  }
}
