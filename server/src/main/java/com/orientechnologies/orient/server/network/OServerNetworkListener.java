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
package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
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

public class OServerNetworkListener extends Thread {
  private OServerSocketFactory socketFactory;
  private ServerSocket serverSocket;
  private InetSocketAddress inboundAddr;
  private Class<? extends ONetworkProtocol> protocolType;
  private volatile boolean active = true;
  private List<OServerCommandConfiguration> statefulCommands =
      new ArrayList<OServerCommandConfiguration>();
  private List<OServerCommand> statelessCommands = new ArrayList<OServerCommand>();
  private int socketBufferSize;
  private OContextConfiguration configuration;
  private OServer server;
  private int protocolVersion = -1;

  public OServerNetworkListener(
      final OServer iServer,
      final OServerSocketFactory iSocketFactory,
      final String iHostName,
      final String iHostPortRange,
      final String iProtocolName,
      final Class<? extends ONetworkProtocol> iProtocol,
      final OServerParameterConfiguration[] iParameters,
      final OServerCommandConfiguration[] iCommands) {
    super(
        iServer.getThreadGroup(),
        "OrientDB " + iProtocol.getSimpleName() + " listen at " + iHostName + ":" + iHostPortRange);
    server = iServer;

    socketFactory = iSocketFactory == null ? OServerSocketFactory.getDefault() : iSocketFactory;

    // DETERMINE THE PROTOCOL VERSION BY CREATING A NEW ONE AND THEN THROW IT AWAY
    // TODO: CREATE PROTOCOL FACTORIES INSTEAD
    try {
      protocolVersion = iProtocol.getConstructor(OServer.class).newInstance(server).getVersion();
    } catch (Exception e) {
      final String message = "Error on reading protocol version for " + iProtocol;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new ONetworkProtocolException(message), e);
    }

    listen(iHostName, iHostPortRange, iProtocolName, iProtocol);
    protocolType = iProtocol;

    readParameters(iServer.getContextConfiguration(), iParameters);

    if (iCommands != null) {
      for (int i = 0; i < iCommands.length; ++i) {
        if (iCommands[i].stateful)
          // SAVE STATEFUL COMMAND CFG
          registerStatefulCommand(iCommands[i]);
        else
          // EARLY CREATE STATELESS COMMAND
          registerStatelessCommand(OServerNetworkListener.createCommand(server, iCommands[i]));
      }
    }

    start();
  }

  public static int[] getPorts(final String iHostPortRange) {
    int[] ports;

    if (OStringSerializerHelper.contains(iHostPortRange, ',')) {
      // MULTIPLE ENUMERATED PORTS
      String[] portValues = iHostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i) ports[i] = Integer.parseInt(portValues[i]);

    } else if (OStringSerializerHelper.contains(iHostPortRange, '-')) {
      // MULTIPLE RANGE PORTS
      String[] limits = iHostPortRange.split("-");
      int lowerLimit = Integer.parseInt(limits[0]);
      int upperLimit = Integer.parseInt(limits[1]);
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < upperLimit - lowerLimit + 1; ++i) ports[i] = lowerLimit + i;

    } else
      // SINGLE PORT SPECIFIED
      ports = new int[] {Integer.parseInt(iHostPortRange)};
    return ports;
  }

  @SuppressWarnings("unchecked")
  public static OServerCommand createCommand(
      final OServer server, final OServerCommandConfiguration iCommand) {
    try {
      final Constructor<OServerCommand> c =
          (Constructor<OServerCommand>)
              Class.forName(iCommand.implementation)
                  .getConstructor(OServerCommandConfiguration.class);
      final OServerCommand cmd = c.newInstance(new Object[] {iCommand});
      cmd.configure(server);
      return cmd;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot create custom command invoking the constructor: "
              + iCommand.implementation
              + "("
              + iCommand
              + ")",
          e);
    }
  }

  public List<OServerCommandConfiguration> getStatefulCommands() {
    return statefulCommands;
  }

  public List<OServerCommand> getStatelessCommands() {
    return statelessCommands;
  }

  public OServerNetworkListener registerStatelessCommand(final OServerCommand iCommand) {
    statelessCommands.add(iCommand);
    return this;
  }

  public OServerNetworkListener unregisterStatelessCommand(
      final Class<? extends OServerCommand> iCommandClass) {
    for (OServerCommand c : statelessCommands) {
      if (c.getClass().equals(iCommandClass)) {
        statelessCommands.remove(c);
        break;
      }
    }
    return this;
  }

  public OServerNetworkListener registerStatefulCommand(
      final OServerCommandConfiguration iCommand) {
    statefulCommands.add(iCommand);
    return this;
  }

  public OServerNetworkListener unregisterStatefulCommand(
      final OServerCommandConfiguration iCommand) {
    statefulCommands.remove(iCommand);
    return this;
  }

  public void shutdown() {
    this.active = false;

    if (serverSocket != null)
      try {
        serverSocket.close();
      } catch (IOException e) {
      }
  }

  public boolean isActive() {
    return active;
  }

  @Override
  public void run() {
    try {
      Constructor<? extends ONetworkProtocol> constructor =
          protocolType.getConstructor(OServer.class);
      while (active) {
        try {
          // listen for and accept a client connection to serverSocket
          final Socket socket = serverSocket.accept();

          final int max =
              server
                  .getContextConfiguration()
                  .getValueAsInteger(OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS);

          int conns = server.getClientConnectionManager().getTotal();
          if (conns >= max) {
            server.getClientConnectionManager().cleanExpiredConnections();
            conns = server.getClientConnectionManager().getTotal();
            if (conns >= max) {
              // MAXIMUM OF CONNECTIONS EXCEEDED
              OLogManager.instance()
                  .warn(
                      this,
                      "Reached maximum number of concurrent connections (max=%d, current=%d), reject incoming connection from %s",
                      max,
                      conns,
                      socket.getRemoteSocketAddress());
              socket.close();

              // PAUSE CURRENT THREAD TO SLOW DOWN ANY POSSIBLE ATTACK
              Thread.sleep(100);
              continue;
            }
          }

          socket.setPerformancePreferences(0, 2, 1);
          socket.setKeepAlive(true);
          if (socketBufferSize > 0) {
            socket.setSendBufferSize(socketBufferSize);
            socket.setReceiveBufferSize(socketBufferSize);
          }
          // CREATE A NEW PROTOCOL INSTANCE
          final ONetworkProtocol protocol = constructor.newInstance(server);

          // CONFIGURE THE PROTOCOL FOR THE INCOMING CONNECTION
          protocol.config(this, server, socket, configuration);

        } catch (Exception e) {
          if (active) OLogManager.instance().error(this, "Error on client connection", e);
        }
      }
    } catch (NoSuchMethodException e) {
      OLogManager.instance()
          .error(this, "error finding the protocol constructor with the server as parameter", e);
    } finally {
      try {
        if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close();
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

  public String getListeningAddress(final boolean resolveMultiIfcWithLocal) {
    String address = serverSocket.getInetAddress().getHostAddress();
    if (resolveMultiIfcWithLocal && address.equals("0.0.0.0")) {
      try {
        address = OChannel.getLocalIpAddress(true);
      } catch (Exception ex) {
        address = null;
      }
      if (address == null) {
        try {
          address = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          OLogManager.instance().warn(this, "Error resolving current host address", e);
        }
      }
    }

    return address + ":" + serverSocket.getLocalPort();
  }

  public static void main(String[] args) {
    System.out.println(OServerNetworkListener.getLocalHostIp());
  }

  public static String getLocalHostIp() {
    try {
      InetAddress host = InetAddress.getLocalHost();
      InetAddress[] addrs = InetAddress.getAllByName(host.getHostName());
      for (InetAddress addr : addrs) {
        if (!addr.isLoopbackAddress()) {
          return addr.toString();
        }
      }
    } catch (UnknownHostException e) {
      try {
        return OChannel.getLocalIpAddress(true);
      } catch (SocketException e1) {

      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(64);
    builder
        .append(protocolType.getSimpleName())
        .append(" ")
        .append(serverSocket.getLocalSocketAddress())
        .append(":");
    return builder.toString();
  }

  public Object getCommand(final Class<?> iCommandClass) {
    // SEARCH IN STATELESS COMMANDS
    for (OServerCommand cmd : statelessCommands) {
      if (cmd.getClass().equals(iCommandClass)) return cmd;
    }

    // SEARCH IN STATEFUL COMMANDS
    for (OServerCommandConfiguration cmd : statefulCommands) {
      if (cmd.implementation.equals(iCommandClass.getName())) return cmd;
    }

    return null;
  }

  /**
   * Initialize a server socket for communicating with the client.
   *
   * @param iHostPortRange
   * @param iHostName
   */
  private void listen(
      final String iHostName,
      final String iHostPortRange,
      final String iProtocolName,
      Class<? extends ONetworkProtocol> protocolClass) {

    for (int port : getPorts(iHostPortRange)) {
      inboundAddr = new InetSocketAddress(iHostName, port);
      try {
        serverSocket = socketFactory.createServerSocket(port, 0, InetAddress.getByName(iHostName));

        if (serverSocket.isBound()) {
          OLogManager.instance()
              .info(
                  this,
                  "Listening $ANSI{green "
                      + iProtocolName
                      + "} connections on $ANSI{green "
                      + inboundAddr.getAddress().getHostAddress()
                      + ":"
                      + inboundAddr.getPort()
                      + "} (protocol v."
                      + protocolVersion
                      + ", socket="
                      + socketFactory.getName()
                      + ")");

          return;
        }
      } catch (BindException be) {
        OLogManager.instance()
            .warn(this, "Port %s:%d busy, trying the next available...", iHostName, port);
      } catch (SocketException se) {
        OLogManager.instance().error(this, "Unable to create socket", se);
        throw new RuntimeException(se);
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Unable to read data from an open socket", ioe);
        System.err.println("Unable to read data from an open socket.");
        throw new RuntimeException(ioe);
      }
    }

    OLogManager.instance()
        .error(
            this,
            "Unable to listen for connections using the configured ports '%s' on host '%s'",
            null,
            iHostPortRange,
            iHostName);
    throw new OSystemException(
        String.format(
            "Unable to listen for connections using the configured ports '%s' on host '%s'",
            iHostPortRange, iHostName));
  }

  /**
   * Initializes connection parameters by the reading XML configuration. If not specified, get the
   * parameters defined as global configuration.
   *
   * @param iServerConfig
   */
  private void readParameters(
      final OContextConfiguration iServerConfig,
      final OServerParameterConfiguration[] iParameters) {
    configuration = new OContextConfiguration(iServerConfig);

    // SET PARAMETERS
    if (iParameters != null && iParameters.length > 0) {
      // CONVERT PARAMETERS IN MAP TO INTIALIZE THE CONTEXT-CONFIGURATION
      for (OServerParameterConfiguration param : iParameters)
        configuration.setValue(param.name, param.value);
    }

    socketBufferSize =
        configuration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
  }
}
