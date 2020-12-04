/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OCloseRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/** Created by tglman on 31/03/16. */
public class OStorageRemoteSession {
  public boolean commandExecuting = false;
  protected int serverURLIndex = -1;
  protected String connectionUserName = null;
  protected String connectionUserPassword = null;
  protected Map<String, OStorageRemoteNodeSession> sessions =
      new HashMap<String, OStorageRemoteNodeSession>();

  private Set<OChannelBinary> connections =
      Collections.newSetFromMap(new WeakHashMap<OChannelBinary, Boolean>());
  private final int uniqueClientSessionId;
  private boolean closed = true;
  /**
   * Make the retry to happen only on the current session, if the current session is invalid or the
   * server is offline it kill the operation.
   *
   * <p>this is for avoid to send to the server wrong request expecting a specific state that is not
   * there anymore.
   */
  private int stickToSession = 0;

  protected String currentUrl;

  public OStorageRemoteSession(final int sessionId) {
    this.uniqueClientSessionId = sessionId;
  }

  public boolean hasConnection(final OChannelBinary connection) {
    return connections.contains(connection);
  }

  public OStorageRemoteNodeSession getServerSession(final String serverURL) {
    return sessions.get(serverURL);
  }

  public synchronized OStorageRemoteNodeSession getOrCreateServerSession(final String serverURL) {
    OStorageRemoteNodeSession session = sessions.get(serverURL);
    if (session == null) {
      session = new OStorageRemoteNodeSession(serverURL, uniqueClientSessionId);
      sessions.put(serverURL, session);
      closed = false;
    }
    return session;
  }

  public void addConnection(final OChannelBinary connection) {
    connections.add(connection);
  }

  public void close() {
    commandExecuting = false;
    serverURLIndex = -1;
    connections = new HashSet<OChannelBinary>();
    sessions = new HashMap<String, OStorageRemoteNodeSession>();
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  public Integer getSessionId() {
    if (sessions.isEmpty()) return -1;
    final OStorageRemoteNodeSession curSession = sessions.values().iterator().next();
    return curSession.getSessionId();
  }

  public String getServerUrl() {
    if (sessions.isEmpty()) return null;
    final OStorageRemoteNodeSession curSession = sessions.values().iterator().next();
    return curSession.getServerURL();
  }

  public synchronized void removeServerSession(final String serverURL) {
    sessions.remove(serverURL);
  }

  public synchronized Collection<OStorageRemoteNodeSession> getAllServerSessions() {
    return sessions.values();
  }

  public void stickToSession() {
    this.stickToSession++;
  }

  public void unStickToSession() {
    this.stickToSession--;
  }

  public boolean isStickToSession() {
    return stickToSession > 0;
  }

  public void closeAllSessions(
      ORemoteConnectionManager connectionManager, OContextConfiguration clientConfiguration) {
    for (OStorageRemoteNodeSession nodeSession : getAllServerSessions()) {
      OChannelBinaryAsynchClient network = null;
      try {
        network =
            OStorageRemote.getNetwork(
                nodeSession.getServerURL(), connectionManager, clientConfiguration);
        OCloseRequest request = new OCloseRequest();
        network.beginRequest(request.getCommand(), this);
        request.write(network, this);
        network.endRequest();
        connectionManager.release(network);
      } catch (OIOException ex) {
        // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
        OLogManager.instance()
            .debug(this, "Impossible to comunicate to the server for close: %s", ex);
        connectionManager.remove(network);
      } catch (IOException ex) {
        // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
        OLogManager.instance()
            .debug(this, "Impossible to comunicate to the server for close: %s", ex);
        connectionManager.remove(network);
      }
    }
    close();
  }

  public String getDebugLastHost() {
    return currentUrl;
  }

  public String getCurrentUrl() {
    return currentUrl;
  }
}
