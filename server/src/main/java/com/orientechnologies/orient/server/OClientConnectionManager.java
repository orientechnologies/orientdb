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
package com.orientechnologies.orient.server;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLSocket;

public class OClientConnectionManager {
  private static final long TIMEOUT_PUSH = 3000;

  protected final ConcurrentMap<Integer, OClientConnection> connections =
      new ConcurrentHashMap<Integer, OClientConnection>();
  protected AtomicInteger connectionSerial = new AtomicInteger(0);
  protected final ConcurrentMap<OHashToken, OClientSessions> sessions =
      new ConcurrentHashMap<OHashToken, OClientSessions>();
  protected final TimerTask timerTask;
  private OServer server;

  public OClientConnectionManager(OServer server) {
    final int delay = OGlobalConfiguration.SERVER_CHANNEL_CLEAN_DELAY.getValueAsInteger();

    timerTask =
        Orient.instance()
            .scheduleTask(
                () -> {
                  try {
                    cleanExpiredConnections();
                  } catch (Exception e) {
                    OLogManager.instance().debug(this, "Error on client connection purge task", e);
                  }
                },
                delay,
                delay);

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "server.connections.actives",
            "Number of active network connections",
            METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return (long) connections.size();
              }
            });
    this.server = server;
  }

  public void cleanExpiredConnections() {
    final Iterator<Entry<Integer, OClientConnection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, OClientConnection> entry = iterator.next();

      if (entry.getValue().tryAcquireForExpire()) {
        try {

          final Socket socket;
          if (entry.getValue().getProtocol() == null
              || entry.getValue().getProtocol().getChannel() == null) socket = null;
          else socket = entry.getValue().getProtocol().getChannel().socket;

          if (socket == null || socket.isClosed() || socket.isInputShutdown()) {
            OLogManager.instance()
                .debug(
                    this,
                    "[OClientConnectionManager] found and removed pending closed channel %d (%s)",
                    entry.getKey(),
                    socket);
            try {
              OCommandRequestText command = entry.getValue().getData().command;
              if (command != null && command.isIdempotent()) {
                entry.getValue().getProtocol().sendShutdown();
                entry.getValue().getProtocol().interrupt();
              }
              removeConnectionFromSession(entry.getValue());
              entry.getValue().close();

            } catch (Exception e) {
              OLogManager.instance()
                  .error(this, "Error during close of connection for close channel", e);
            }
            iterator.remove();
          } else if (Boolean.TRUE.equals(entry.getValue().getTokenBased())) {
            if (entry.getValue().getToken() != null
                && !server.getTokenHandler().validateBinaryToken(entry.getValue().getToken())) {
              // Close the current session but not the network because can be used by another
              // session.
              removeConnectionFromSession(entry.getValue());
              entry.getValue().close();
              iterator.remove();
            }
          }
        } finally {
          entry.getValue().release();
        }
      }
    }
    server.getPushManager().cleanPushSockets();
  }

  /**
   * Create a connection.
   *
   * @param iProtocol protocol which will be used by connection
   * @return new connection
   */
  public OClientConnection connect(final ONetworkProtocol iProtocol) {

    final OClientConnection connection;

    connection = new OClientConnection(connectionSerial.incrementAndGet(), iProtocol);

    connections.put(connection.getId(), connection);
    OLogManager.instance().config(this, "Remote client connected from: " + connection);
    OServerPluginHelper.invokeHandlerCallbackOnClientConnection(iProtocol.getServer(), connection);
    return connection;
  }

  /**
   * Create a connection.
   *
   * @param iProtocol protocol which will be used by connection
   * @return new connection
   */
  public OClientConnection connect(
      final ONetworkProtocol iProtocol,
      final OClientConnection connection,
      final byte[] tokenBytes) {

    OParsedToken parsedToken;
    try {
      parsedToken = server.getTokenHandler().parseOnlyBinary(tokenBytes);
    } catch (Exception e) {
      throw OException.wrapException(new OTokenSecurityException("Error on token parsing"), e);
    }
    if (!server.getTokenHandler().validateBinaryToken(parsedToken)) {
      throw new OTokenSecurityException("The token provided is expired");
    }
    OClientSessions session;
    synchronized (sessions) {
      session = new OClientSessions(tokenBytes);
      sessions.put(new OHashToken(tokenBytes), session);
    }
    connection.setToken(parsedToken, tokenBytes);
    session.addConnection(connection);
    OLogManager.instance().config(this, "Remote client connected from: " + connection);
    OServerPluginHelper.invokeHandlerCallbackOnClientConnection(iProtocol.getServer(), connection);
    return connection;
  }

  public OClientConnection reConnect(final ONetworkProtocol iProtocol, final byte[] tokenBytes) {
    final OClientConnection connection;
    connection = new OClientConnection(connectionSerial.incrementAndGet(), iProtocol);
    connections.put(connection.getId(), connection);
    OParsedToken parsedToken;
    try {
      parsedToken = server.getTokenHandler().parseOnlyBinary(tokenBytes);
    } catch (Exception e) {
      throw OException.wrapException(new OTokenSecurityException("Error on token parsing"), e);
    }
    if (!server.getTokenHandler().validateBinaryToken(parsedToken)) {
      throw new OTokenSecurityException("The token provided is expired");
    }

    OHashToken key = new OHashToken(tokenBytes);
    OClientSessions sess;
    synchronized (sessions) {
      sess = sessions.get(key);
      if (sess == null) {
        // RECONNECT
        sess = new OClientSessions(tokenBytes);
        sessions.put(new OHashToken(tokenBytes), sess);
      }
    }
    connection.setToken(parsedToken, tokenBytes);
    sess.addConnection(connection);
    OServerPluginHelper.invokeHandlerCallbackOnClientConnection(iProtocol.getServer(), connection);
    return connection;
  }

  /**
   * Retrieves the connection by id.
   *
   * @param iChannelId id of connection
   * @return The connection if any, otherwise null
   */
  public OClientConnection getConnection(final int iChannelId, ONetworkProtocol protocol) {
    // SEARCH THE CONNECTION BY ID
    OClientConnection connection = connections.get(iChannelId);
    if (connection != null) connection.setProtocol(protocol);

    return connection;
  }

  /**
   * Retrieves the connection by address/port.
   *
   * @param iAddress The address as string in the format address as format <ip>:<port>
   * @return The connection if any, otherwise null
   */
  public OClientConnection getConnection(final String iAddress) {
    for (OClientConnection conn : connections.values()) {
      if (iAddress.equals(conn.getRemoteAddress())) return conn;
    }
    return null;
  }

  /**
   * Disconnects and kill the associated network manager.
   *
   * @param iChannelId id of connection
   */
  public void kill(final int iChannelId) {
    kill(connections.get(iChannelId));
  }

  /**
   * Disconnects and kill the associated network manager.
   *
   * @param connection connection to kill
   */
  public void kill(final OClientConnection connection) {
    if (connection != null) {
      final ONetworkProtocol protocol = connection.getProtocol();

      try {
        // INTERRUPT THE NEWTORK MANAGER TOO
        protocol.interrupt();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during interruption of binary protocol", e);
      }

      disconnect(connection);

      // KILL THE NETWORK MANAGER TOO
      protocol.sendShutdown();
    }
  }

  public boolean has(final int id) {
    return connections.containsKey(id);
  }

  /**
   * Interrupt the associated network manager.
   *
   * @param iChannelId id of connection
   */
  public void interrupt(final int iChannelId) {
    final OClientConnection connection = connections.get(iChannelId);
    if (connection != null) {
      final ONetworkProtocol protocol = connection.getProtocol();
      if (protocol != null)
        // INTERRUPT THE NEWTORK MANAGER
        protocol.softShutdown();
    }
  }

  /**
   * Disconnects a client connections
   *
   * @param iChannelId id of connection
   * @return true if was last one, otherwise false
   */
  public boolean disconnect(final int iChannelId) {
    OLogManager.instance().debug(this, "Disconnecting connection with id=%d", iChannelId);

    final OClientConnection connection = connections.remove(iChannelId);

    if (connection != null) {
      OServerPluginHelper.invokeHandlerCallbackOnClientDisconnection(server, connection);
      connection.close();
      removeConnectionFromSession(connection);

      // CHECK IF THERE ARE OTHER CONNECTIONS
      for (Entry<Integer, OClientConnection> entry : connections.entrySet()) {
        if (entry.getValue().getProtocol().equals(connection.getProtocol())) {
          OLogManager.instance()
              .debug(
                  this,
                  "Disconnected connection with id=%d but are present other active channels",
                  iChannelId);
          return false;
        }
      }

      OLogManager.instance()
          .debug(
              this,
              "Disconnected connection with id=%d, no other active channels found",
              iChannelId);
      return true;
    }

    OLogManager.instance().debug(this, "Cannot find connection with id=%d", iChannelId);
    return false;
  }

  private void removeConnectionFromSession(OClientConnection connection) {
    if (connection.getProtocol() instanceof ONetworkProtocolBinary) {
      byte[] tokenBytes = connection.getTokenBytes();
      OHashToken hashToken = new OHashToken(tokenBytes);
      synchronized (sessions) {
        OClientSessions sess = sessions.get(hashToken);
        if (sess != null) {
          sess.removeConnection(connection);
          if (!sess.isActive()) {
            sessions.remove(hashToken);
          }
        }
      }
    }
  }

  public void disconnect(final OClientConnection iConnection) {
    OLogManager.instance().debug(this, "Disconnecting connection %s...", iConnection);
    OServerPluginHelper.invokeHandlerCallbackOnClientDisconnection(server, iConnection);
    removeConnectionFromSession(iConnection);
    iConnection.close();

    int totalRemoved = 0;
    for (Entry<Integer, OClientConnection> entry :
        new HashMap<Integer, OClientConnection>(connections).entrySet()) {
      final OClientConnection conn = entry.getValue();
      if (conn != null && conn.equals(iConnection)) {
        connections.remove(entry.getKey());
        totalRemoved++;
      }
    }

    OLogManager.instance()
        .debug(this, "Disconnected connection %s found %d channels", iConnection, totalRemoved);
  }

  public List<OClientConnection> getConnections() {
    return new ArrayList<OClientConnection>(connections.values());
  }

  public int getTotal() {
    return connections.size();
  }

  /** Pushes the distributed configuration to all the connected clients. */
  public void pushDistribCfg2Clients(final ODocument iConfig) {
    if (iConfig == null) return;

    final Set<String> pushed = new HashSet<String>();
    for (OClientConnection c : connections.values()) {
      if (!c.getData().supportsLegacyPushMessages) continue;

      try {
        final String remoteAddress = c.getRemoteAddress();
        if (pushed.contains(remoteAddress))
          // ALREADY SENT: JUMP IT
          continue;

      } catch (Exception e) {
        // SOCKET EXCEPTION SKIP IT
        continue;
      }

      if (!(c.getProtocol() instanceof ONetworkProtocolBinary)
          || c.getData().getSerializationImpl() == null)
        // INVOLVE ONLY BINARY PROTOCOLS
        continue;

      final ONetworkProtocolBinary p = (ONetworkProtocolBinary) c.getProtocol();
      final OChannelBinary channel = p.getChannel();
      final ORecordSerializer ser =
          ORecordSerializerFactory.instance().getFormat(c.getData().getSerializationImpl());
      if (ser == null) return;

      final byte[] content = ser.toStream(iConfig);

      try {
        // TRY ACQUIRING THE LOCK FOR MAXIMUM 3 SECS TO AVOID TO FREEZE CURRENT THREAD
        if (channel.tryAcquireWriteLock(TIMEOUT_PUSH)) {
          try {
            channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
            channel.writeInt(Integer.MIN_VALUE);
            channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG);
            channel.writeBytes(content);
            channel.flush();

            pushed.add(c.getRemoteAddress());
            OLogManager.instance()
                .debug(
                    this,
                    "Sent updated cluster configuration to the remote client %s",
                    c.getRemoteAddress());

          } finally {
            channel.releaseWriteLock();
          }
        } else {
          OLogManager.instance()
              .info(
                  this,
                  "Timeout on sending updated cluster configuration to the remote client %s",
                  c.getRemoteAddress());
        }
      } catch (Exception e) {
        OLogManager.instance()
            .warn(
                this,
                "Cannot push cluster configuration to the client %s",
                e,
                c.getRemoteAddress());
      }
    }
  }

  public void shutdown() {
    timerTask.cancel();

    List<ONetworkProtocol> toWait = new ArrayList<ONetworkProtocol>();

    final Iterator<Entry<Integer, OClientConnection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, OClientConnection> entry = iterator.next();

      final ONetworkProtocol protocol = entry.getValue().getProtocol();

      if (protocol != null) protocol.sendShutdown();

      OLogManager.instance().debug(this, "Sending shutdown to thread %s", protocol);

      OCommandRequestText command = entry.getValue().getData().command;
      if (command != null && command.isIdempotent()) {
        protocol.interrupt();
      } else {
        if (protocol instanceof ONetworkProtocolBinary
            && ((ONetworkProtocolBinary) protocol).getRequestType()
                == OChannelBinaryProtocol.REQUEST_SHUTDOWN) {
          continue;
        }

        final Socket socket;
        if (protocol == null || protocol.getChannel() == null) socket = null;
        else socket = protocol.getChannel().socket;

        if (socket != null && !socket.isClosed() && !socket.isInputShutdown()) {
          try {
            OLogManager.instance().debug(this, "Closing input socket of thread %s", protocol);
            if (!(socket
                instanceof SSLSocket)) // An SSLSocket will throw an UnsupportedOperationException.
            socket.shutdownInput();
          } catch (IOException e) {
            OLogManager.instance()
                .debug(
                    this,
                    "Error on closing connection of %s client during shutdown",
                    e,
                    entry.getValue().getRemoteAddress());
          }
        }
        if (protocol.isAlive()) {
          if (protocol instanceof ONetworkProtocolBinary
              && ((ONetworkProtocolBinary) protocol).getRequestType() == -1) {
            try {
              OLogManager.instance().debug(this, "Closing socket of thread %s", protocol);
              protocol.getChannel().close();
            } catch (Exception e) {
              OLogManager.instance().debug(this, "Error during chanel close at shutdown", e);
            }
            OLogManager.instance().debug(this, "Sending interrupt signal to thread %s", protocol);
            protocol.interrupt();
          }
          toWait.add(protocol);
        }
      }
    }

    for (ONetworkProtocol protocol : toWait) {
      try {
        protocol.join(
            server
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.SERVER_CHANNEL_CLEAN_DELAY));
        if (protocol.isAlive()) {
          protocol.interrupt();
          protocol.join();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void killAllChannels() {
    for (Map.Entry<Integer, OClientConnection> entry : connections.entrySet()) {
      try {
        ONetworkProtocol protocol = entry.getValue().getProtocol();

        protocol.getChannel().close();

        final Socket socket;
        if (protocol == null || protocol.getChannel() == null) socket = null;
        else socket = protocol.getChannel().socket;

        if (socket != null && !socket.isClosed() && !socket.isInputShutdown()) {
          if (!(socket
              instanceof SSLSocket)) // An SSLSocket will throw an UnsupportedOperationException.
          socket.shutdownInput();
        }

      } catch (Exception e) {
        OLogManager.instance()
            .debug(
                this,
                "Error on killing connection to %s client",
                e,
                entry.getValue().getRemoteAddress());
      }
    }
  }

  public OClientSessions getSession(OClientConnection connection) {
    OHashToken key = new OHashToken(connection.getTokenBytes());
    synchronized (sessions) {
      return sessions.get(key);
    }
  }
}
