package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;

import java.io.IOException;
import java.net.*;
import java.util.*;

public class OMulticastNodeDiscoveryManager extends ONodeManager {

  private static final int BUFFER_SIZE = 1024;

  private final String multicastIp;
  private       int    listeningPort;

  MulticastSocket socket;
  private       Thread discoveryThread;
  private final int[]  discoveryPorts;

  protected long discoveryPingIntervalMillis = 1000;//TODO configure

  /**
   * @param oDistributedNetworkManager
   * @param multicastIp
   * @param listeningPort
   * @param taskScheduler
   */
  public OMulticastNodeDiscoveryManager(String groupName, String nodeName, int quorum,
      ODiscoveryListener oDistributedNetworkManager, int listeningPort, String multicastIp, int[] multicastDiscoveryPorts,
      OSchedulerInternal taskScheduler) {
    super(taskScheduler, groupName, nodeName, quorum, 0, oDistributedNetworkManager); //TODO term (from OpLog...?)!!

    this.listeningPort = listeningPort;
    this.multicastIp = multicastIp;
    this.discoveryPorts = multicastDiscoveryPorts;

  }

  public void start() {
    super.start();
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
    taskScheduler.scheduleOnce(new TimerTask() {
      public void run() {
        try {
          checkIfKnownServersAreAlive();
          if (running) {
            initCheckDisconnect();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, discoveryPingIntervalMillis);
  }

  /**
   * init the procedure that sends pings to other servers, ie. that notifies that you are alive
   */
  private void initDiscoveryPing() {
    taskScheduler.scheduleOnce(new TimerTask() {
      @Override
      public void run() {
        try {
          sendPing();
          if (running) {
            initDiscoveryPing();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, discoveryPingIntervalMillis);
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

  private void sendPing() throws Exception {
    if (!running) {
      return;
    }
    Message ping = generatePingMessage();
    byte[] msg = serializeMessage(ping);
    sendMessageToGroup(msg);
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

  private synchronized void checkIfKnownServersAreAlive() {
    Set<String> toRemove = new HashSet<>();
    for (Map.Entry<String, ODiscoveryListener.NodeData> entry : knownServers.entrySet()) {
      if (entry.getValue().lastPingTimestamp < System.currentTimeMillis() - maxInactiveServerTimeMillis) {
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(x -> {
      ODiscoveryListener.NodeData val = knownServers.remove(x);
      discoveryListener.nodeLeft(val);
    });
  }

  private synchronized Message generatePingMessage() {
    //nodeData
    Message message = new Message();
    message.type = Message.TYPE_PING;
    message.nodeName = this.nodeName;
    message.group = group;
    message.term = leaderStatus.currentTerm;
    message.role =
        leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER ? Message.ROLE_COORDINATOR : Message.ROLE_REPLICA;

    //masterData
    ODiscoveryListener.NodeData master = this.knownServers.values().stream().filter(x -> x.master).findFirst().orElse(null);
    if (master != null) {
      message.masterName = master.name;
      message.masterTerm = master.term;
      message.masterAddress = master.address;
      message.masterTcpPort = master.port;
      message.masterPing = master.lastPingTimestamp;
    }

    return message;
  }

  private void receiveMessages() {
    try {
      byte[] buffer = new byte[BUFFER_SIZE];
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      if (socket.isClosed()) {
        return;
      }
      socket.receive(packet);
      packet.getAddress();
      Message message = deserializeMessage(packet.getData());
      if (!message.group.equals(group)) {
        return;
      }
      String fromAddr = packet.getAddress().getHostAddress();
      processMessage(message, fromAddr);
    } catch (SocketException ex) {
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void stop() {
    running = false;
    socket.close();
  }

}
