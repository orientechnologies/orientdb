package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
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
    String group;
    String nodeName;
  }

  private static final int BUFFER_SIZE = 1024;

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

  private String group;
  private String groupPassword;
  private String encryptionAlgorithm = "AES";

  private boolean running = true;

  /**
   * @param oDistributedNetworkManager
   * @param multicastIp
   * @param listeningPort
   * @param taskScheduler
   */
  public OMulticastNodeDiscoveryManager(String groupName, String nodeName, ODiscoveryListener oDistributedNetworkManager,
      int listeningPort, String multicastIp, int[] multicastDiscoveryPorts, OSchedulerInternal taskScheduler) {
    if (groupName == null || groupName.length() == 0) {
      throw new IllegalArgumentException("Invalid group name");
    }
    this.group = groupName;
    if (nodeName == null || nodeName.length() == 0) {
      throw new IllegalArgumentException("Invalid node name");
    }
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
    message.group = group;
    //TODO
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
    } catch (SocketException ex) {
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private byte[] serializeMessage(Message message) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    buffer.write(message.type);

    switch (message.type) {
    case Message.TYPE_PING:
      buffer.write(message.tcpPort);
      buffer.write(message.role);
      writeString(message.group, buffer);
      writeString(message.nodeName, buffer);
      break;
    }

    return encrypt(buffer.toByteArray());
  }

  private Message deserializeMessage(byte[] data) throws Exception {
    data = decrypt(data);
    if (data == null) {
      return null;
    }
    Message message = new Message();
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    message.type = stream.read();
    switch (message.type) {
    case Message.TYPE_PING:
      message.tcpPort = stream.read();
      message.role = stream.read();
      message.group = readString(stream);
      message.nodeName = readString(stream);
    }
    return message;
  }

  private void writeString(String string, ByteArrayOutputStream buffer) throws IOException {
    if (string == null) {
      buffer.write(-1);
      return;
    }
    buffer.write(string.length());
    if (string.length() == 0) {
      return;
    }
    buffer.write(string.getBytes());
  }

  private String readString(ByteArrayInputStream stream) throws IOException {
    int length = stream.read();
    if (length < 0) {
      return null;
    }
    if (length == 0) {
      return "";
    }
    byte[] nameBuffer = new byte[length];
    stream.read(nameBuffer);
    return new String(nameBuffer);
  }

  private byte[] encrypt(byte[] data) throws Exception {
    if (groupPassword == null) {
      return data;
    }
    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    byte[] iv = cipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    SecretKeySpec keySpec = new SecretKeySpec(paddedPassword(groupPassword), "AES");
    cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(iv.length);
    stream.write(iv);
    byte[] cypher = cipher.doFinal(data);
    stream.write(cypher.length);
    stream.write(cypher);

    return stream.toByteArray();
  }

  private byte[] decrypt(byte[] data) throws Exception {
    if (groupPassword == null) {
      return data;
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    int ivLength = stream.read();
    byte[] ivData = new byte[ivLength];
    stream.read(ivData);
    IvParameterSpec ivSpec = new IvParameterSpec(ivData);
    SecretKeySpec skeySpec = new SecretKeySpec(paddedPassword(groupPassword), "AES");

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

    int length = stream.read();
    byte[] encrypted = new byte[length];
    stream.read(encrypted);
    return cipher.doFinal(encrypted);
  }

  private byte[] paddedPassword(String pwd) {
    if (pwd == null) {
      return null;
    }
    while (pwd.length() < 16) {
      pwd += "=";
    }
    if (pwd.length() > 16) {
      pwd = pwd.substring(16);
    }
    return pwd.getBytes();
  }

  public void stop() {
    running = false;
    socket.close();
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getGroupPassword() {
    return groupPassword;
  }

  public void setGroupPassword(String groupPassword) {
    this.groupPassword = groupPassword;
  }

  public String getEncryptionAlgorithm() {
    return encryptionAlgorithm;
  }

  public void setEncryptionAlgorithm(String encryptionAlgorithm) {
    this.encryptionAlgorithm = encryptionAlgorithm;
  }
}
