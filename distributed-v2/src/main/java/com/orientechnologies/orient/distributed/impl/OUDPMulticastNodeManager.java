package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;

public class OUDPMulticastNodeManager extends ONodeManager {

  private static final int BUFFER_SIZE = 1024;

  private final String multicastIp;
  private       int    listeningPort;

  MulticastSocket socket;
  private final int[] discoveryPorts;

  /**
   * @param config
   * @param oDistributedNetworkManager
   * @param taskScheduler
   */
  public OUDPMulticastNodeManager(ONodeConfiguration config, ONodeInternalConfiguration internalConfiguration,
      ODiscoveryListener oDistributedNetworkManager, OSchedulerInternal taskScheduler) {
    super(config, internalConfiguration, 0, taskScheduler, oDistributedNetworkManager); //TODO term (from OpLog...?)!!

    this.listeningPort = config.getMulticast().getPort();
    this.multicastIp = config.getMulticast().getIp();
    this.discoveryPorts = config.getMulticast().getDiscoveryPorts();

  }

  public void start() {
    super.start();
  }

  public void stop() {
    super.stop();
    socket.close();
  }

  protected void initNetwork() throws IOException {
    socket = new MulticastSocket(listeningPort);
    InetAddress group = InetAddress.getByName(multicastIp);
    try {
      socket.joinGroup(group);
    } catch (Exception e) {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        try {
          NetworkInterface iface = interfaces.nextElement();
          socket.setNetworkInterface(iface);
          socket.joinGroup(group);
          OLogManager.instance().info(this, "Switched to network interface '" + iface.getName() + "' for multicast");
          break;
        } catch (Exception e2) {
        }
        if (!interfaces.hasMoreElements()) {
          OLogManager.instance().error(this, "Cannot initialize multicast socket, "
              + "probably the problem is due to default IPv6 settings for current network interface. "
              + "Please try to start the process with -Djava.net.preferIPv4Stack=true", null);
        }
      }
    }
  }

  protected void sendMessageToGroup(byte[] msg) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    InetAddress group = InetAddress.getByName(this.multicastIp);
    for (int discoveryPort : discoveryPorts) {
      DatagramPacket packet = new DatagramPacket(msg, msg.length, group, discoveryPort);
      socket.send(packet);
    }
    socket.close();
  }

  protected void receiveMessages() {
    try {
      byte[] buffer = new byte[BUFFER_SIZE];
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      if (socket.isClosed()) {
        return;
      }
      socket.receive(packet);
      packet.getAddress();
      Message message = deserializeMessage(packet.getData());
      if (!message.group.equals(this.config.getGroupName())) {
        return;
      }
      String fromAddr = packet.getAddress().getHostAddress();
      processMessage(message, fromAddr);
    } catch (SocketException ex) {
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
