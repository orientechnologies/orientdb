/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server;

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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynch;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

public class OClientConnectionManager {
  protected ConcurrentMap<Integer, OClientConnection> connections      = new ConcurrentHashMap<Integer, OClientConnection>();
  protected AtomicInteger                             connectionSerial = new AtomicInteger(0);

  private static final OClientConnectionManager       instance         = new OClientConnectionManager();

  public OClientConnectionManager() {
    final int delay = OGlobalConfiguration.SERVER_CHANNEL_CLEAN_DELAY.getValueAsInteger();

    Orient.instance().getTimer().schedule(new TimerTask() {

      @Override
      public void run() {
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
              entry.getValue().close();
            } catch (Exception e) {
            }
            iterator.remove();
          }
        }
      }
    }, delay, delay);

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
  public OClientConnection getConnection(final int iChannelId) {
    // SEARCH THE CONNECTION BY ID
    return connections.get(iChannelId);

    // COMMENTED TO USE SOCKET POOL: THINK TO ANOTHER WAY TO IMPROVE SECURITY
    // if (conn != null && conn.getChannel().socket != socket)
    // throw new IllegalStateException("Requested sessionId " + iChannelId + " by connection " + socket
    // + " while it's tied to connection " + conn.getChannel().socket);
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
      disconnect(connection);

      // KILL THE NEWTORK MANAGER TOO
      protocol.sendShutdown();
    }
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
        // INTERRUPT THE NEWTORK MANAGER TOO
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

  public static OClientConnectionManager instance() {
    return instance;
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
    final byte[] content = iConfig.toStream();

    final Set<String> pushed = new HashSet<String>();
    for (OClientConnection c : connections.values()) {
      if (pushed.contains(c.getRemoteAddress()))
        // ALREADY SENT: JUMP IT
        continue;

      if (!(c.protocol instanceof ONetworkProtocolBinary))
        // INVOLVE ONLY BINAR PROTOCOLS
        continue;

      final ONetworkProtocolBinary p = (ONetworkProtocolBinary) c.protocol;
      final OChannelBinary channel = (OChannelBinary) p.getChannel();

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

  /**
   * Pushes the record to all the connected clients with the same database.
   * 
   * @param iRecord
   *          Record to broadcast
   * @param iExcludeConnection
   *          Connection to exclude if any, usually the current where the change has been just applied
   */
  public void pushRecord2Clients(final ORecordInternal<?> iRecord, final OClientConnection iExcludeConnection)
      throws InterruptedException, IOException {
    final String dbName = iRecord.getDatabase().getName();

    for (OClientConnection c : connections.values()) {
      if (c != iExcludeConnection) {
        final ONetworkProtocolBinary p = (ONetworkProtocolBinary) c.protocol;
        final OChannelBinaryAsynch channel = (OChannelBinaryAsynch) p.getChannel();

        if (c.database != null && c.database.getName().equals(dbName))
          synchronized (c) {
            try {
              channel.acquireWriteLock();
              try {
                channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
                channel.writeInt(Integer.MIN_VALUE);
                channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_RECORD);
                p.writeIdentifiable(iRecord);
              } finally {
                channel.releaseWriteLock();
              }
            } catch (IOException e) {
              OLogManager.instance().warn(this, "Cannot push record to the client %s", c.getRemoteAddress());
            }

          }

      }
    }

  }
}
