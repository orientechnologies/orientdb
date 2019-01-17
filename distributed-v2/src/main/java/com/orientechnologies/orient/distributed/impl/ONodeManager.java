package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public abstract class ONodeManager {

  protected static class Message {
    static final int TYPE_PING          = 0;
    static final int TYPE_LEAVE         = 1;
    static final int TYPE_KNOWN_SERVERS = 2;

    static final int TYPE_START_LEADER_ELECTION = 3;
    static final int TYPE_VOTE_LEADER_ELECTION  = 4;
    static final int TYPE_LEADER_ELECTED        = 5;

    static final int ROLE_COORDINATOR = 0;
    static final int ROLE_REPLICA     = 1;
    static final int ROLE_UNDEFINED   = 2;

    int    type;
    String nodeName;
    String group;
    int    term;
    int    role;
    String connectionUsername;
    String connectionPassword;

    //for ping
    int tcpPort;

    // for leader election
    String voteForNode;
    String dbName;
    long   lastLogId;

    //MASTER INFO

    String masterName;
    int    masterTerm;
    String masterAddress;
    int    masterTcpPort;
    String masterConnectionUsername;
    String masterConnectionPassword;
    long   masterPing;
  }

  protected boolean running = true;

  protected final ODiscoveryListener discoveryListener;
  private         Thread             messageThread;

  protected Map<String, ODiscoveryListener.NodeData> knownServers;

  protected final ONodeConfiguration config;

  private String encryptionAlgorithm = "AES";

  protected final OSchedulerInternal taskScheduler;

  protected long discoveryPingIntervalMillis = 1000;//TODO configure
  protected long checkLeaderIntervalMillis   = 1000;//TODO configure
  /**
   * max time a server can be silent (did not get ping from it) until it is considered inactive, ie. left the network
   */
  protected long maxInactiveServerTimeMillis = 5000;

  OLeaderElectionStateMachine leaderStatus;

  public ONodeManager(ONodeConfiguration config, int term, OSchedulerInternal taskScheduler, ODiscoveryListener discoveryListener) {
    this.config = config;
    if (config.getGroupName() == null || config.getGroupName().length() == 0) {
      throw new IllegalArgumentException("Invalid group name");
    }

    if (config.getNodeName() == null || config.getNodeName().length() == 0) {
      throw new IllegalArgumentException("Invalid node name");
    }
    this.discoveryListener = discoveryListener;
    knownServers = new HashMap<>();
    this.taskScheduler = taskScheduler;
    leaderStatus = new OLeaderElectionStateMachine();
    leaderStatus.nodeName = config.getNodeName();
    leaderStatus.setQuorum(config.getQuorum());
    leaderStatus.changeTerm(term);
  }

  /**
   * initializes the network layer
   *
   * @throws IOException
   */
  protected abstract void initNetwork() throws IOException;

  /**
   * sends messages to all the members of the network (current group)
   *
   * @param msg
   *
   * @throws IOException
   */
  protected abstract void sendMessageToGroup(byte[] msg) throws IOException;

  /**
   * receives messages from the network. It is supposed to invoke {@link #processMessage(Message, String)} for each message
   * received
   */
  protected abstract void receiveMessages();





  /* =============== START/STOP ================= */

  protected void start() {
    try {
      initNetwork();
      initReceiveMessages();
      initDiscoveryPing();
      initCheckLeader();
      initCheckDisconnect();
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Cannot start distributed node discovery: " + ex.getMessage(), ex);
    }
  }

  public void stop() {
    running = false;
  }





  /* =============== MESSAGING ================= */

  /**
   * inits the procedure that listens to pings from other servers, eg. that discovers other nodes in the network
   *
   * @throws IOException
   */
  protected void initReceiveMessages() throws IOException {
    messageThread = new Thread(() -> {
      while (running) {
        receiveMessages();
      }
    });
    messageThread.setName("OrientDB_DistributedDiscoveryThread");
    messageThread.setDaemon(true);
    messageThread.start();
  }

  protected synchronized void processMessage(Message message, String fromAddr) {
//    System.out.println(
//        "MSG toNode: " + this.nodeName + " fromNode: " + message.nodeName + " role: " + message.role + " term: " + message.term
//            + " type: " + message.type + " master: " + message.masterName + " masterTerm: " + message.masterTerm+" masterPing: "+message.masterPing);
    switch (message.type) {
    case Message.TYPE_PING:
//      System.out.println("" + nodeName + " - RECEIVE PING FROM " + message.nodeName);
      processReceivePing(message, fromAddr);
      break;
    case Message.TYPE_START_LEADER_ELECTION:
//      System.out.println("" + nodeName + " - RECEIVE START ELECTION FROM " + message.nodeName);
      processReceiveStartElection(message, fromAddr);
      break;
    case Message.TYPE_VOTE_LEADER_ELECTION:
//      System.out.println("" + nodeName + " - RECEIVE VOTE LEADER FROM " + message.nodeName);
      processReceiveVote(message, fromAddr);
      break;
    case Message.TYPE_LEADER_ELECTED:
//      System.out.println("" + nodeName + " - RECEIVE LEADER ELECTED FROM " + message.nodeName);
      processReceiveLeaderElected(message, fromAddr);
      break;
    }

  }



  /* =============== PING ================= */

  /**
   * init the procedure that sends pings to other servers, ie. that notifies that you are alive
   */
  protected void initDiscoveryPing() {
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

  protected void sendPing() throws Exception {
    if (!running) {
      return;
    }
    Message ping = generatePingMessage();
    byte[] msg = serializeMessage(ping);
    sendMessageToGroup(msg);
  }

  protected synchronized Message generatePingMessage() {
    //nodeData
    Message message = new Message();
    message.type = Message.TYPE_PING;
    message.nodeName = this.config.getNodeName();
    message.group = this.config.getGroupName();
    message.term = leaderStatus.currentTerm;
    message.role =
        leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER ? Message.ROLE_COORDINATOR : Message.ROLE_REPLICA;

    message.connectionUsername = config.getConnectionUsername();
    message.connectionPassword = config.getConnectionPassword();
    message.tcpPort = config.getTcpPort();
    //masterData
    ODiscoveryListener.NodeData master = this.knownServers.values().stream().filter(x -> x.master).findFirst().orElse(null);
    if (master != null) {
      message.masterName = master.name;
      message.masterTerm = master.term;
      message.masterAddress = master.address;
      message.masterTcpPort = master.port;
      message.masterConnectionUsername = master.connectionUsername;
      message.masterConnectionPassword = master.connectionPassword;
      message.masterPing = master.lastPingTimestamp;
    }

    return message;
  }

  protected void processReceivePing(Message message, String fromAddr) {
    synchronized (knownServers) {
      if (leaderStatus.currentTerm > message.term) {
        return;
      }
      boolean wasLeader = false;
      ODiscoveryListener.NodeData data = knownServers.get(message.nodeName);
      if (data == null) {
        data = new ODiscoveryListener.NodeData();
        data.term = message.term;
        data.name = message.nodeName;
        data.address = fromAddr;
        data.connectionUsername = message.connectionUsername;
        data.connectionPassword = message.connectionPassword;
        data.port = message.tcpPort;
        knownServers.put(message.nodeName, data);
        discoveryListener.nodeJoined(data);
      } else if (data.master) {
        wasLeader = true;
      }
      data.lastPingTimestamp = System.currentTimeMillis();
      if (data.term < message.term) {
        data.term = message.term;
        if (message.role == Message.ROLE_COORDINATOR) {
          resetLeader();
          data.master = true;
        } else {
          data.master = false;
        }
        leaderStatus.changeTerm(message.term);
        if (this.config.getNodeName().equals(message.masterName)) {
          leaderStatus.status = OLeaderElectionStateMachine.Status.LEADER;
        }
      } else if (data.term == message.term && message.role == Message.ROLE_COORDINATOR) {
        resetLeader();
        data.master = true;
        if (!message.nodeName.equals(this.config.getNodeName())) {
          leaderStatus.status = OLeaderElectionStateMachine.Status.FOLLOWER;
        }
      }
      if (data.master && !wasLeader) {
        discoveryListener.leaderElected(data);
      }

      //Master info
      if (message.masterName != null && message.masterTerm >= this.leaderStatus.currentTerm
          && message.masterPing + maxInactiveServerTimeMillis > System.currentTimeMillis()) {
        data = knownServers.get(message.masterName);

        if (data == null) {
          data = new ODiscoveryListener.NodeData();
          data.name = message.masterName;
          data.term = message.masterTerm;
          data.address = message.masterAddress;
          data.connectionUsername = message.masterConnectionUsername;
          data.connectionPassword = message.masterConnectionPassword;
          data.port = message.masterTcpPort;
          data.lastPingTimestamp = message.masterPing;
          data.master = true;
          knownServers.put(message.masterName, data);
          discoveryListener.nodeJoined(data);
          discoveryListener.leaderElected(data);
        }

      }
    }
  }






  /* =============== CHECK SERVERS ALIVE ================= */

  /**
   * inits the procedure that checks if a server is no longer available, ie. if he did not ping for a long time
   */
  protected void initCheckDisconnect() {
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






  /* =============== LEADER ELECTION ================= */

  /**
   * init the procedure that sends pings to other servers, ie. that notifies that you are alive
   */
  private void initCheckLeader() {
    taskScheduler.scheduleOnce(new TimerTask() {
      @Override
      public void run() {
        try {
          if (running) {
            checkLeader();
            initCheckLeader();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, checkLeaderIntervalMillis);
  }

  private void checkLeader() {
    synchronized (this) {
      if (leaderStatus.status == OLeaderElectionStateMachine.Status.CANDIDATE) {
        leaderStatus.resetLeaderElection();
      }
      if (knownServers.size() < leaderStatus.quorum) {
        //no reason to do a leader election in this condition
        return;
      }

      for (ODiscoveryListener.NodeData node : knownServers.values()) {
        if (node.master && node.term >= leaderStatus.currentTerm) {
          return; //master found
        }
      }
    }
    try {
      Thread.sleep((int) (Math.random() * 2000));
    } catch (InterruptedException e) {
    }

    synchronized (this) {
      for (ODiscoveryListener.NodeData node : knownServers.values()) {
        if (node.master && node.term >= leaderStatus.currentTerm) {
          return; //master found
        }
      }

      if (leaderStatus.status == OLeaderElectionStateMachine.Status.FOLLOWER) {
        leaderStatus.startElection();
        sendStartElection(leaderStatus.currentTerm, null, 0);
      }
    }
  }

  protected void sendStartElection(int currentTerm, String dbName, long lastLogId) {
//    System.out.println("" + this.nodeName + " * START ELECTION term " + currentTerm + " node " + nodeName);
    Message message = new Message();
    message.group = this.config.getGroupName();
    message.nodeName = this.config.getNodeName();
    message.term = currentTerm;
    message.dbName = dbName;
    message.lastLogId = lastLogId;
    message.type = Message.TYPE_START_LEADER_ELECTION;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  private void processReceiveLeaderElected(Message message, String fromAddr) {
    if (message.term >= leaderStatus.currentTerm) {
      if (!this.config.getNodeName().equals(message.nodeName)) {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
      } else {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.LEADER);
      }

      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.name = message.nodeName;
      data.master = true;
      data.term = message.term;
      data.address = fromAddr;
      data.connectionUsername = message.connectionUsername;
      data.connectionPassword = message.connectionPassword;
      data.port = message.tcpPort;
      data.lastPingTimestamp = System.currentTimeMillis();

      ODiscoveryListener.NodeData oldEntry = this.knownServers.put(data.name, data);
      if (oldEntry == null) {
        discoveryListener.nodeJoined(data);
      }

      discoveryListener.leaderElected(data);
    }
  }

  protected void processReceiveStartElection(Message message, String fromAddr) {
    if (message.term > leaderStatus.currentTerm && message.term > leaderStatus.lastTermVoted) {
      //vote, but only once per term!
      leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
      leaderStatus.lastTermVoted = message.term;
      sendVote(message.term, message.nodeName);
    }
  }

  private void sendVote(int term, String toNode) {
    Message message = new Message();
    message.group = this.config.getGroupName();
    message.nodeName = this.config.getNodeName();
    message.term = term;
    message.voteForNode = toNode;
    message.type = Message.TYPE_VOTE_LEADER_ELECTION;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  protected void processReceiveVote(Message message, String fromAddr) {
//    System.out.println("RECEIVE VOTE term " + message.term + " from " + message.nodeName + " to " + message.voteForNode);
    if (leaderStatus.status != OLeaderElectionStateMachine.Status.CANDIDATE) {
      return;
    }
    leaderStatus.receiveVote(message.term, message.nodeName, message.voteForNode);
    if (leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER) {
      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.term = leaderStatus.currentTerm;
      data.master = true;
      data.name = this.config.getNodeName();
      data.lastPingTimestamp = System.currentTimeMillis();
      discoveryListener.leaderElected(data);
      knownServers.put(this.config.getNodeName(), data);
      sendLeaderElected();
    }
  }

  private void resetLeader() {
    knownServers.values().forEach(x -> x.master = false);
  }

  private void sendLeaderElected() {
//    System.out.println("SEND LEADER ELECTED " + nodeName);
    Message message = new Message();
    message.group = this.config.getGroupName();
    message.nodeName = this.config.getNodeName();
    message.term = leaderStatus.currentTerm;
    //message.tcpPort = //TODO
    message.type = Message.TYPE_LEADER_ELECTED;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }








  /* =============== NETWORK UTILITIES ================= */

  protected byte[] serializeMessage(Message message) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    writeInt(message.type, buffer);
    writeString(message.group, buffer);
    writeString(message.nodeName, buffer);
    writeInt(message.term, buffer);
    writeInt(message.role, buffer);
    writeInt(message.tcpPort, buffer);
    writeString(message.connectionUsername, buffer);
    writeString(message.connectionPassword, buffer);

    switch (message.type) {
    case Message.TYPE_PING:
      writeString(message.masterName, buffer);
      writeInt(message.masterTerm, buffer);
      writeString(message.masterAddress, buffer);
      writeInt(message.masterTcpPort, buffer);
      writeLong(message.masterPing, buffer);
      writeString(message.masterConnectionUsername, buffer);
      writeString(message.masterConnectionPassword, buffer);
      break;
    case Message.TYPE_VOTE_LEADER_ELECTION:
      writeString(message.voteForNode, buffer);
    }

    return encrypt(buffer.toByteArray());
  }

  protected Message deserializeMessage(byte[] data) throws Exception {
    data = decrypt(data);
    if (data == null) {
      return null;
    }
    Message message = new Message();
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    message.type = readInt(stream);
    message.group = readString(stream);
    message.nodeName = readString(stream);
    message.term = readInt(stream);
    message.role = readInt(stream);
    message.tcpPort = readInt(stream);
    message.connectionUsername = readString(stream);
    message.connectionPassword = readString(stream);

    switch (message.type) {
    case Message.TYPE_PING:
      message.masterName = readString(stream);
      message.masterTerm = readInt(stream);
      message.masterAddress = readString(stream);
      message.masterTcpPort = readInt(stream);
      message.masterPing = readLong(stream);
      message.masterConnectionUsername = readString(stream);
      message.masterConnectionPassword = readString(stream);

    case Message.TYPE_VOTE_LEADER_ELECTION:
      message.voteForNode = readString(stream);
    }
    return message;
  }

  private int readInt(ByteArrayInputStream buffer) throws IOException {
    byte[] r = new byte[4];
    buffer.read(r);
    return ByteBuffer.wrap(r).getInt();
  }

  private void writeInt(int i, ByteArrayOutputStream buffer) throws IOException {
    buffer.write(ByteBuffer.allocate(4).putInt(i).array());
  }

  private long readLong(ByteArrayInputStream buffer) throws IOException {
    byte[] r = new byte[8];
    buffer.read(r);
    return ByteBuffer.wrap(r).getLong();
  }

  private void writeLong(Long i, ByteArrayOutputStream buffer) throws IOException {
    buffer.write(ByteBuffer.allocate(8).putLong(i).array());
  }

  private String readString(ByteArrayInputStream stream) throws IOException {
    int length = readInt(stream);
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

  private void writeString(String string, ByteArrayOutputStream buffer) throws IOException {
    if (string == null) {
      writeInt(-1, buffer);
      return;
    }
    writeInt(string.length(), buffer);
    if (string.length() == 0) {
      return;
    }
    buffer.write(string.getBytes());
  }






  /* =============== ENCRYPTION ================= */

  private byte[] encrypt(byte[] data) throws Exception {
    if (config.getGroupPassword() == null) {
      return data;
    }
    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    byte[] iv = cipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    SecretKeySpec keySpec = new SecretKeySpec(paddedPassword(config.getGroupPassword()), "AES");
    cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    writeInt(iv.length, stream);
    stream.write(iv);
    byte[] cypher = cipher.doFinal(data);
    writeInt(cypher.length, stream);
    stream.write(cypher);

    return stream.toByteArray();
  }

  private byte[] decrypt(byte[] data) throws Exception {
    if (config.getGroupPassword() == null) {
      return data;
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    int ivLength = readInt(stream);
    byte[] ivData = new byte[ivLength];
    stream.read(ivData);
    IvParameterSpec ivSpec = new IvParameterSpec(ivData);
    SecretKeySpec skeySpec = new SecretKeySpec(paddedPassword(config.getGroupPassword()), "AES");

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

    int length = readInt(stream);
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





  /* =============== GETTERS/SETTERS ================= */

  public ODiscoveryListener getDiscoveryListener() {
    return discoveryListener;
  }

  public ONodeConfiguration getConfig() {
    return config;
  }
}
