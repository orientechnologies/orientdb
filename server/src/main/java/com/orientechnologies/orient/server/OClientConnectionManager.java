/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OClientConnectionManager {
  private static final OClientConnectionManager       instance         = new OClientConnectionManager();
  protected ConcurrentMap<Integer, OClientConnection> connections      = new ConcurrentHashMap<Integer, OClientConnection>();
  protected AtomicInteger                             connectionSerial = new AtomicInteger(0);

  public OClientConnectionManager() {
    final int delay = OGlobalConfiguration.SERVER_CHANNEL_CLEAN_DELAY.getValueAsInteger();

    Orient.instance().scheduleTask(new TimerTask() {

      @Override
      public void run() {
        cleanExpiredConnections();
      }
    }

    , delay, delay);

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("server.connections.actives", "Number of active network connections", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return connections.size();
              }
            });
  }

  public static OClientConnectionManager instance() {
    return instance;
  }

  public void cleanExpiredConnections() {
    final Iterator<Entry<Integer, OClientConnection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, OClientConnection> entry = iterator.next();

      final Socket socket;
      if (entry.getValue().protocol == null || entry.getValue().protocol.getChannel() == null)
        socket = null;
      else
        socket = entry.getValue().protocol.getChannel().socket;

      if (socket == null || socket.isClosed() || socket.isInputShutdown()) {
        OLogManager.instance().debug(this, "[OClientConnectionManager] found and removed pending closed channel %d (%s)",
            entry.getKey(), socket);
        try {
          OCommandRequestText command = entry.getValue().data.command;
          if (command != null && command.isIdempotent()) {
            entry.getValue().protocol.sendShutdown();
            entry.getValue().protocol.interrupt();
          }
          entry.getValue().close();

        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during close of connection for close channel", e);
        }
        iterator.remove();
      }
    }
  }

  /**
   * Create a connection.
   * 
   * @param iProtocol
   *          protocol which will be used by connection
   * @return new connection
   * @throws IOException
   */
  public OClientConnection connect(final ONetworkProtocol iProtocol) throws IOException {

    final OClientConnection connection;

    connection = new OClientConnection(connectionSerial.incrementAndGet(), iProtocol);

    connections.put(connection.id, connection);

    OLogManager.instance().config(this, "Remote client connected from: " + connection);

    return connection;
  }

  /**
   * Retrieves the connection by id.
   * 
   * @param iChannelId
   *          id of connection
   * @return The connection if any, otherwise null
   */
  public OClientConnection getConnection(final int iChannelId, ONetworkProtocol protocol) {
    // SEARCH THE CONNECTION BY ID
    OClientConnection connection = connections.get(iChannelId);
    if (connection != null)
      connection.protocol = protocol;

    return connection;
  }

  /**
   * Retrieves the connection by address/port.
   * 
   * @param iAddress
   *          The address as string in the format address as format <ip>:<port>
   * @return The connection if any, otherwise null
   */
  public OClientConnection getConnection(final String iAddress) {
    for (OClientConnection conn : connections.values()) {
      if (iAddress.equals(conn.getRemoteAddress()))
        return conn;
    }
    return null;
  }

  /**
   * Disconnects and kill the associated network manager.
   * 
   * @param iChannelId
   *          id of connection
   */
  public void kill(final int iChannelId) {
    kill(connections.get(iChannelId));
  }

  /**
   * Disconnects and kill the associated network manager.
   * 
   * @param connection
   *          connection to kill
   */
  public void kill(final OClientConnection connection) {
    if (connection != null) {
      final ONetworkProtocol protocol = connection.protocol;

      try {
        // INTERRUPT THE NEWTORK MANAGER TOO
        protocol.interrupt();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during interruption of binary protocol.", e);
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
   * @param iChannelId
   *          id of connection
   */
  public void interrupt(final int iChannelId) {
    final OClientConnection connection = connections.get(iChannelId);
    if (connection != null) {
      final ONetworkProtocol protocol = connection.protocol;
      if (protocol != null)
        // INTERRUPT THE NEWTORK MANAGER
        protocol.interrupt();
    }
  }

  /**
   * Disconnects a client connections
   * 
   * @param iChannelId
   *          id of connection
   * @return true if was last one, otherwise false
   */
  public boolean disconnect(final int iChannelId) {
    OLogManager.instance().debug(this, "Disconnecting connection with id=%d", iChannelId);

    final OClientConnection connection = connections.remove(iChannelId);

    if (connection != null) {
      connection.close();

      // CHECK IF THERE ARE OTHER CONNECTIONS
      for (Entry<Integer, OClientConnection> entry : connections.entrySet()) {
        if (entry.getValue().getProtocol().equals(connection.getProtocol())) {
          OLogManager.instance()
              .debug(this, "Disconnected connection with id=%d but are present other active channels", iChannelId);
          return false;
        }
      }

      OLogManager.instance().debug(this, "Disconnected connection with id=%d, no other active channels found", iChannelId);
      return true;
    }

    OLogManager.instance().debug(this, "Cannot find connection with id=%d", iChannelId);
    return false;
  }

  public void disconnect(final OClientConnection iConnection) {
    OLogManager.instance().debug(this, "Disconnecting connection %s...", iConnection);

    iConnection.close();

    int totalRemoved = 0;
    for (Entry<Integer, OClientConnection> entry : new HashMap<Integer, OClientConnection>(connections).entrySet()) {
      final OClientConnection conn = entry.getValue();
      if (conn != null && conn.equals(iConnection)) {
        connections.remove(entry.getKey());
        totalRemoved++;
      }
    }

    OLogManager.instance().debug(this, "Disconnected connection %s found %d channels", iConnection, totalRemoved);

  }

  public List<OClientConnection> getConnections() {
    return new ArrayList<OClientConnection>(connections.values());
  }

  public int getTotal() {
    return connections.size();
  }

  /**
   * Pushes the distributed configuration to all the connected clients.
   */
  public void pushDistribCfg2Clients(final ODocument iConfig) {
    if (iConfig == null)
      return;

    final Set<String> pushed = new HashSet<String>();
    for (OClientConnection c : connections.values()) {
      try {
        final String remoteAddress = c.getRemoteAddress();
        if (pushed.contains(remoteAddress))
          // ALREADY SENT: JUMP IT
          continue;

      } catch (Exception e) {
        // SOCKET EXCEPTION SKIP IT
        continue;
      }

      if (!(c.protocol instanceof ONetworkProtocolBinary) || c.data.serializationImpl == null)
        // INVOLVE ONLY BINARY PROTOCOLS
        continue;

      final ONetworkProtocolBinary p = (ONetworkProtocolBinary) c.protocol;
      final OChannelBinary channel = (OChannelBinary) p.getChannel();
      final ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(c.data.serializationImpl);
      if (ser == null)
        return;

      final byte[] content = ser.toStream(iConfig, false);

      try {
        channel.acquireWriteLock();
        try {
          channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
          channel.writeInt(Integer.MIN_VALUE);
          channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG);
          channel.writeBytes(content);
          channel.flush();

          pushed.add(c.getRemoteAddress());
          OLogManager.instance().info(this, "Sent updated cluster configuration to the remote client %s", c.getRemoteAddress());

        } finally {
          channel.releaseWriteLock();
        }
      } catch (IOException e) {
        disconnect(c);
      } catch (Exception e) {
        OLogManager.instance().warn(this, "Cannot push cluster configuration to the client %s", e, c.getRemoteAddress());
        disconnect(c);
      }
    }
  }

  public void shutdown() {

    final Iterator<Entry<Integer, OClientConnection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, OClientConnection> entry = iterator.next();
      entry.getValue().protocol.sendShutdown();

      OCommandRequestText command = entry.getValue().data.command;
      if (command != null && command.isIdempotent()) {
        entry.getValue().protocol.interrupt();
      } else {
        ONetworkProtocol protocol = entry.getValue().protocol;
        if (protocol instanceof ONetworkProtocolBinary
            && ((ONetworkProtocolBinary) protocol).getRequestType() == OChannelBinaryProtocol.REQUEST_SHUTDOWN) {
          continue;
        }

        try {
          final Socket socket;
          if (entry.getValue().protocol == null || entry.getValue().protocol.getChannel() == null)
            socket = null;
          else
            socket = entry.getValue().protocol.getChannel().socket;

          if (socket != null && !socket.isClosed() && !socket.isInputShutdown()) {
            try {
              socket.shutdownInput();
            } catch (IOException e) {
              OLogManager.instance().warn(this, "Error on closing connection of %s client during shutdown", e,
                  entry.getValue().getRemoteAddress());
            }
          }
          if (entry.getValue().protocol.isAlive()) {
            if (entry.getValue().protocol instanceof ONetworkProtocolBinary
                && ((ONetworkProtocolBinary) entry.getValue().protocol).getRequestType() == -1) {
              try {
                entry.getValue().protocol.getChannel().close();
              } catch (Exception e) {
                OLogManager.instance().error(this, "Error during chanel close at shutdown", e);
              }
              entry.getValue().protocol.interrupt();
            }

            entry.getValue().protocol.join();
          }
        } catch (InterruptedException e) {
          // NOT Needed to handle
        }
      }
    }
  }
}
