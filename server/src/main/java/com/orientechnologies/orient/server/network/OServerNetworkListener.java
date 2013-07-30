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
package com.orientechnologies.orient.server.network;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;

public class OServerNetworkListener extends Thread {
  private ServerSocket                      serverSocket;
  private InetSocketAddress                 inboundAddr;
  private Class<? extends ONetworkProtocol> protocolType;
  private volatile boolean                  active            = true;
  private List<OServerCommandConfiguration> statefulCommands  = new ArrayList<OServerCommandConfiguration>();
  private List<OServerCommand>              statelessCommands = new ArrayList<OServerCommand>();
  private int                               socketBufferSize;
  private OContextConfiguration             configuration;
  private OServer                           server;
  private ONetworkProtocol                  protocol;
  private int                               protocolVersion   = -1;

  public OServerNetworkListener(final OServer iServer, final String iHostName, final String iHostPortRange,
      final String iProtocolName, final Class<? extends ONetworkProtocol> iProtocol,
      final OServerParameterConfiguration[] iParameters, final OServerCommandConfiguration[] iCommands) {
    super(Orient.instance().getThreadGroup(), "OrientDB " + iProtocol.getSimpleName() + " listen at " + iHostName + ":"
        + iHostPortRange);
    server = iServer;

    // DETERMINE THE PROTOCOL VERSION BY CREATING A NEW ONE AND THEN THROW IT AWAY
    // TODO: CREATE PROTOCOL FACTORIES INSTEAD
    try {
      protocolVersion = iProtocol.newInstance().getVersion();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on reading protocol version for %s", e, ONetworkProtocolException.class,
          protocolType);
    }

    listen(iHostName, iHostPortRange, iProtocolName);
    protocolType = iProtocol;

    readParameters(iServer.getContextConfiguration(), iParameters);

    if (iCommands != null) {
      for (int i = 0; i < iCommands.length; ++i) {
        if (iCommands[i].stateful)
          // SAVE STATEFUL COMMAND CFG
          statefulCommands.add(iCommands[i]);
        else
          // EARLY CREATE STATELESS COMMAND
          statelessCommands.add(OServerNetworkListener.createCommand(server, iCommands[i]));
      }
    }

    start();
  }

  public void shutdown() {
    this.active = false;

    if (protocol != null) {
      protocol.sendShutdown();
      protocol = null;
    }

    if (serverSocket != null)
      try {
        serverSocket.close();
      } catch (IOException e) {
      }
  }

  /**
   * Initialize a server socket for communicating with the client.
   * 
   * @param iHostPortRange
   * @param iHostName
   */
  private void listen(final String iHostName, final String iHostPortRange, final String iProtocolName) {
    final int[] ports = getPorts(iHostPortRange);

    for (int port : ports) {
      inboundAddr = new InetSocketAddress(iHostName, port);
      try {
        serverSocket = new java.net.ServerSocket(port, 0, InetAddress.getByName(iHostName));

        if (serverSocket.isBound()) {
          OLogManager.instance().info(
              this,
              "Listening " + iProtocolName + " connections on " + inboundAddr.getAddress().getHostAddress() + ":"
                  + inboundAddr.getPort() + " (protocol v." + protocolVersion + ")");
          return;
        }
      } catch (BindException be) {
        OLogManager.instance().info(this, "Port %s:%d busy, trying the next available...", iHostName, port);
      } catch (SocketException se) {
        OLogManager.instance().error(this, "Unable to create socket", se);
        System.exit(1);
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Unable to read data from an open socket", ioe);
        System.err.println("Unable to read data from an open socket.");
        System.exit(1);
      }
    }

    OLogManager.instance().error(this, "Unable to listen for connections using the configured ports '%s' on host '%s'",
        iHostPortRange, iHostName);
    System.exit(1);
  }

  public boolean isActive() {
    return active;
  }

  @Override
  public void run() {
    try {
      while (active) {
        try {
          // listen for and accept a client connection to serverSocket
          final Socket socket = serverSocket.accept();

          final int conns = OClientConnectionManager.instance().getTotal();
          if (conns >= OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger()) {
            // MAXIMUM OF CONNECTIONS EXCEEDED
            OLogManager.instance().warn(this,
                "Reached maximum number of concurrent connections (%d), reject incoming connection from %s", conns,
                socket.getRemoteSocketAddress());
            socket.close();

            // PAUSE CURRENT THREAD TO SLOW DOWN ANY POSSIBLE ATTACK
            Thread.sleep(100);
          }

          socket.setPerformancePreferences(0, 2, 1);
          socket.setSendBufferSize(socketBufferSize);
          socket.setReceiveBufferSize(socketBufferSize);

          // CREATE A NEW PROTOCOL INSTANCE
          protocol = protocolType.newInstance();

          // CONFIGURE THE PROTOCOL FOR THE INCOMING CONNECTION
          protocol.config(server, socket, configuration, statelessCommands, statefulCommands);

        } catch (Throwable e) {
          if (active)
            OLogManager.instance().error(this, "Error on client connection", e);
        } finally {
        }
      }
    } finally {
      try {
        if (serverSocket != null && !serverSocket.isClosed())
          serverSocket.close();
        protocol = null;
      } catch (IOException ioe) {
      }
    }
  }

  public Class<? extends ONetworkProtocol> getProtocolType() {
    return protocolType;
  }

  public InetSocketAddress getInboundAddr() {
    return inboundAddr;
  }

  /**
   * Initializes connection parameters by the reading XML configuration. If not specified, get the parameters defined as global
   * configuration.
   * 
   * @param iServerConfig
   */
  private void readParameters(final OContextConfiguration iServerConfig, final OServerParameterConfiguration[] iParameters) {
    configuration = new OContextConfiguration(iServerConfig);

    // SET PARAMETERS
    if (iParameters != null && iParameters.length > 0) {
      // CONVERT PARAMETERS IN MAP TO INTIALIZE THE CONTEXT-CONFIGURATION
      for (OServerParameterConfiguration param : iParameters)
        configuration.setValue(param.name, param.value);
    }

    socketBufferSize = configuration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
  }

  public static int[] getPorts(final String iHostPortRange) {
    int[] ports;

    if (OStringSerializerHelper.contains(iHostPortRange, ',')) {
      // MULTIPLE ENUMERATED PORTS
      String[] portValues = iHostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i)
        ports[i] = Integer.parseInt(portValues[i]);

    } else if (OStringSerializerHelper.contains(iHostPortRange, '-')) {
      // MULTIPLE RANGE PORTS
      String[] limits = iHostPortRange.split("-");
      int lowerLimit = Integer.parseInt(limits[0]);
      int upperLimit = Integer.parseInt(limits[1]);
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < upperLimit - lowerLimit + 1; ++i)
        ports[i] = lowerLimit + i;

    } else
      // SINGLE PORT SPECIFIED
      ports = new int[] { Integer.parseInt(iHostPortRange) };
    return ports;
  }

  public String getListeningAddress() {
    String address = serverSocket.getInetAddress().getHostAddress().toString();
    if (address.equals("0.0.0.0"))
      try {
        address = InetAddress.getLocalHost().getHostAddress().toString();
      } catch (UnknownHostException e) {
      }
    return address + ":" + serverSocket.getLocalPort();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(protocolType.getSimpleName()).append(" ").append(serverSocket.getLocalSocketAddress()).append(":");
    return builder.toString();
  }

  public ONetworkProtocol getProtocol() {
    return protocol;
  }

  @SuppressWarnings("unchecked")
  public static OServerCommand createCommand(final OServer server, final OServerCommandConfiguration iCommand) {
    try {
      final Constructor<OServerCommand> c = (Constructor<OServerCommand>) Class.forName(iCommand.implementation).getConstructor(
          OServerCommandConfiguration.class);
      final OServerCommand cmd = c.newInstance(new Object[] { iCommand });
      cmd.configure(server);
      return cmd;
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot create custom command invoking the constructor: " + iCommand.implementation + "("
          + iCommand + ")", e);
    }
  }
}
