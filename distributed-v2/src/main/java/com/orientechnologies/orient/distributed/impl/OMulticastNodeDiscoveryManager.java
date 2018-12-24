package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

public class OMulticastNodeDiscoveryManager {

  private class Message {
    static final int TYPE_PING          = 0;
    static final int TYPE_LEAVE         = 1;
    static final int TYPE_KNOWN_SERVERS = 2;

    static final int ROLE_COORDINATOR = 0;
    static final int ROLE_REPLICA     = 1;
    static final int ROLE_UNDEFINED   = 2;

    int    type;
    int    tcpPort;
    int    role;
    String nodeName;
  }

  private final String                                   nodeName;
  private       Map<String, ODiscoveryListener.NodeData> knownServers;
  private final String                                   multicastIp;
  private       int                                      listeningPort;
  private final ODiscoveryListener                       networkManager;
  MulticastSocket socket;
  private       Thread             discoveryThread;
  private final int[]              discoveryPorts;
  private final OSchedulerInternal taskScheduler;
  private       long               discoveryPingIntervalMillis = 1000;//TODO configure
  /**
   * max time a server can be silent (did not get ping from it) until it is considered inactive, ie. left the network
   */
  private       long               maxInactiveServerTimeMillis = 10000;

  private boolean running = true;

  /**
   * @param oDistributedNetworkManager
   * @param multicastIp
   * @param listeningPort
   * @param taskScheduler
   */
  public OMulticastNodeDiscoveryManager(String nodeName, ODiscoveryListener oDistributedNetworkManager, int listeningPort,
      String multicastIp, int[] multicastDiscoveryPorts, OSchedulerInternal taskScheduler) {
    this.nodeName = nodeName;
    this.networkManager = oDistributedNetworkManager;
    this.listeningPort = listeningPort;
    this.multicastIp = multicastIp;
    this.discoveryPorts = multicastDiscoveryPorts;
    this.taskScheduler = taskScheduler;
    knownServers = new HashMap<>();
  }

  public void start() {
    try {
      initDiscoveryListening();
      initDiscoveryPing();
      initCheckDisconnect();
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Cannot start distributed node discovery: " + ex.getMessage(), ex);
    }
  }

  /**
   * inits the procedure that checks if a server is no longer available, ie. if he did not ping for a long time
   */
  private void initCheckDisconnect() {
    taskScheduler.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          checkIfKnownServersAreAlive();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, discoveryPingIntervalMillis, discoveryPingIntervalMillis);
  }

  /**
   * init the procedure that sends pings to other servers, ie. that notifies that you are alive
   */
  private void initDiscoveryPing() {
    taskScheduler.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          sendPing();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, discoveryPingIntervalMillis, discoveryPingIntervalMillis);
  }

  /**
   * inits the procedure that listens to pings from other servers, eg. that discovers other nodes in the network
   *
   * @throws IOException
   */
  private void initDiscoveryListening() throws IOException {
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
    discoveryThread = new Thread(() -> {
      while (running) {
        receiveMessages();
      }
    });
    discoveryThread.setName("OrientDB_DistributedDiscoveryThread");
    discoveryThread.setDaemon(true);
    discoveryThread.start();
  }

  private void sendPing() throws IOException {
    if (!running) {
      return;
    }
    DatagramSocket socket = new DatagramSocket();
    InetAddress group = InetAddress.getByName(this.multicastIp);
    Message ping = generatePingMessage();
    byte[] msg = serializeMessage(ping);
    for (int discoveryPort : discoveryPorts) {
      DatagramPacket packet = new DatagramPacket(msg, msg.length, group, discoveryPort);
      socket.send(packet);
    }
    socket.close();
  }

  private void checkIfKnownServersAreAlive() {
    synchronized (knownServers) {
      Set<String> toRemove = new HashSet<>();
      for (Map.Entry<String, ODiscoveryListener.NodeData> entry : knownServers.entrySet()) {
        if (entry.getValue().lastPingTimestamp < System.currentTimeMillis() - maxInactiveServerTimeMillis) {
          toRemove.add(entry.getKey());
        }
      }
      toRemove.forEach(x -> {
        ODiscoveryListener.NodeData val = knownServers.remove(x);
        networkManager.nodeLeft(val);
      });
    }
  }

  private Message generatePingMessage() {
    Message message = new Message();
    message.type = Message.TYPE_PING;
    message.nodeName = this.nodeName;
    //TODO
    return message;
  }

  private void receiveMessages() {
    try {
      byte[] buffer = new byte[1024];
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      socket.receive(packet);
      packet.getAddress();
      Message message = deserializeMessage(packet.getData());
      if (message.type == Message.TYPE_PING) {
        synchronized (knownServers) {
          ODiscoveryListener.NodeData data = knownServers.get(message.nodeName);
          if (data == null) {
            data = new ODiscoveryListener.NodeData();
            data.name = message.nodeName;
            data.address = packet.getAddress().getHostAddress();
            data.port = message.tcpPort;
            knownServers.put(message.nodeName, data);
            networkManager.nodeJoined(data);
          }
          data.lastPingTimestamp = System.currentTimeMillis();
        }
      }
    } catch (Exception e) {

    }
  }

  private byte[] serializeMessage(Message message) {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    buffer.putInt(message.type);

    switch (message.type) {
    case Message.TYPE_PING:
      buffer.putInt(message.tcpPort);
      buffer.putInt(message.role);
      buffer.putInt(message.nodeName.length());
      for (char aByte : message.nodeName.toCharArray()) {
        buffer.putChar(aByte);
      }
      break;
    }
    return buffer.array();
  }

  private Message deserializeMessage(byte[] data) {
    Message message = new Message();
    ByteBuffer buffer = ByteBuffer.wrap(data);
    message.type = buffer.getInt();
    switch (message.type) {
    case Message.TYPE_PING:
      message.tcpPort = buffer.getInt();
      message.role = buffer.getInt();
      int nodeNameLength = buffer.getInt();
      message.nodeName = "";
      for (int i = 0; i < nodeNameLength; i++) {
        message.nodeName += buffer.getChar();
      }
    }
    return message;
  }

  public void stop() {
    running = false;
    socket.close();
  }

}
