package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public abstract class ONodeManager {

  protected boolean running = true;

  protected final ODiscoveryListener discoveryListener;
  private         Thread             messageThread;

  protected Map<ONodeIdentity, ODiscoveryListener.NodeData> knownServers;

  protected final ONodeConfiguration         config;
  protected final ONodeInternalConfiguration internalConfiguration;

  private String encryptionAlgorithm = "AES";

  protected final OSchedulerInternal taskScheduler;

  protected long discoveryPingIntervalMillis = 1000;//TODO configure
  protected long checkLeaderIntervalMillis   = 1000;//TODO configure
  /**
   * max time a server can be silent (did not get ping from it) until it is considered inactive, ie. left the network
   */
  protected long maxInactiveServerTimeMillis = 5000;

  OLeaderElectionStateMachine leaderStatus;
  private TimerTask                   discoveryTimer;
  private TimerTask                   disconnectTimer;
  private TimerTask                   checkerTimer;

  public ONodeManager(ONodeConfiguration config, ONodeInternalConfiguration internalConfiguration, int term,
      OSchedulerInternal taskScheduler, ODiscoveryListener discoveryListener) {
    this.config = config;
    this.internalConfiguration = internalConfiguration;
    if (config.getGroupName() == null || config.getGroupName().length() == 0) {
      throw new IllegalArgumentException("Invalid group name");
    }

    if (config.getNodeIdentity().getName() == null || config.getNodeIdentity().getName().length() == 0) {
      throw new IllegalArgumentException("Invalid node name");
    }
    this.discoveryListener = discoveryListener;
    knownServers = new HashMap<>();
    this.taskScheduler = taskScheduler;
    leaderStatus = new OLeaderElectionStateMachine();
    leaderStatus.nodeIdentity = config.getNodeIdentity();
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
   * receives messages from the network. It is supposed to invoke {@link #processMessage(OBroadcastMessage, String)} for each
   * message received
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
    checkerTimer.cancel();
    disconnectTimer.cancel();
    discoveryTimer.cancel();
    try {
      do {
        messageThread.interrupt();
        messageThread.join(1000);
      } while (messageThread.isAlive());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }





  /* =============== MESSAGING ================= */

  /**
   * inits the procedure that listens to pings from other servers, eg. that discovers other nodes in the network
   *
   * @throws IOException
   */
  protected void initReceiveMessages() throws IOException {
    messageThread = new Thread(() -> {
      while (!Thread.interrupted()) {
        receiveMessages();
      }
    });
    messageThread.setName("OrientDB_DistributedDiscoveryThread");
    messageThread.setDaemon(true);
    messageThread.start();
  }

  protected synchronized void processMessage(OBroadcastMessage message, String fromAddr) {
//    System.out.println(
//        "MSG toNode: " + this.nodeName + " fromNode: " + message.nodeName + " role: " + message.role + " term: " + message.term
//            + " type: " + message.type + " master: " + message.masterName + " masterTerm: " + message.masterTerm+" masterPing: "+message.masterPing);
    switch (message.type) {
    case OBroadcastMessage.TYPE_PING:
//      System.out.println("" + nodeName + " - RECEIVE PING FROM " + message.nodeName);
      processReceivePing(message, fromAddr);
      break;
    case OBroadcastMessage.TYPE_START_LEADER_ELECTION:
//      System.out.println("" + nodeName + " - RECEIVE START ELECTION FROM " + message.nodeName);
      processReceiveStartElection(message, fromAddr);
      break;
    case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
//      System.out.println("" + nodeName + " - RECEIVE VOTE LEADER FROM " + message.nodeName);
      processReceiveVote(message, fromAddr);
      break;
    case OBroadcastMessage.TYPE_LEADER_ELECTED:
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
    discoveryTimer = new TimerTask() {
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
    };
    taskScheduler.scheduleOnce(discoveryTimer, discoveryPingIntervalMillis);
  }

  protected void sendPing() throws Exception {
    if (!running) {
      return;
    }
    OBroadcastMessage ping = generatePingMessage();
    byte[] msg = serializeMessage(ping);
    sendMessageToGroup(msg);
  }

  protected synchronized OBroadcastMessage generatePingMessage() {
    //nodeData
    OBroadcastMessage message = new OBroadcastMessage();
    message.type = OBroadcastMessage.TYPE_PING;
    message.nodeIdentity = this.config.getNodeIdentity();
    message.group = this.config.getGroupName();
    message.term = leaderStatus.currentTerm;
    message.role = leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER ?
        OBroadcastMessage.ROLE_COORDINATOR :
        OBroadcastMessage.ROLE_REPLICA;

    message.connectionUsername = internalConfiguration.getConnectionUsername();
    message.connectionPassword = internalConfiguration.getConnectionPassword();
    message.tcpPort = config.getTcpPort();
    //masterData
    ODiscoveryListener.NodeData master = this.knownServers.values().stream().filter(x -> x.master).findFirst().orElse(null);
    if (master != null) {
      message.masterIdentity = master.getNodeIdentity();
      message.masterTerm = master.term;
      message.masterAddress = master.address;
      message.masterTcpPort = master.port;
      message.masterConnectionUsername = master.connectionUsername;
      message.masterConnectionPassword = master.connectionPassword;
      message.masterPing = master.lastPingTimestamp;
    }

    return message;
  }

  protected void processReceivePing(OBroadcastMessage message, String fromAddr) {
    synchronized (knownServers) {
      if (leaderStatus.currentTerm > message.term) {
        return;
      }
      boolean wasLeader = false;
      ODiscoveryListener.NodeData data = knownServers.get(message.getNodeIdentity());
      if (data == null) {
        data = message.toNodeData();
        //TODO: this should be removed and should be get from the message
        data.address = fromAddr;
        knownServers.put(message.getNodeIdentity(), data);
        discoveryListener.nodeConnected(data);
      } else if (data.master) {
        wasLeader = true;
      }
      data.lastPingTimestamp = System.currentTimeMillis();
      if (data.term < message.term) {
        data.term = message.term;
        if (message.role == OBroadcastMessage.ROLE_COORDINATOR) {
          resetLeader();
          data.master = true;
        } else {
          data.master = false;
        }
        leaderStatus.changeTerm(message.term);
        if (this.config.getNodeIdentity().equals(message.getNodeIdentity())) {
          leaderStatus.status = OLeaderElectionStateMachine.Status.LEADER;
        }
      } else if (data.term == message.term && message.role == OBroadcastMessage.ROLE_COORDINATOR) {
        resetLeader();
        data.master = true;
        if (!message.getNodeIdentity().equals(this.config.getNodeIdentity())) {
          leaderStatus.status = OLeaderElectionStateMachine.Status.FOLLOWER;
        }
      }
      if (data.master && !wasLeader) {
        discoveryListener.leaderElected(data);
      }

      //Master info
      if (message.masterIdentity != null && message.masterTerm >= this.leaderStatus.currentTerm
          && message.masterPing + maxInactiveServerTimeMillis > System.currentTimeMillis()) {
        data = knownServers.get(message.masterIdentity);

        if (data == null) {
          data = new ODiscoveryListener.NodeData();
          data.identity = message.masterIdentity;
          data.term = message.masterTerm;
          data.address = message.masterAddress;
          data.connectionUsername = message.masterConnectionUsername;
          data.connectionPassword = message.masterConnectionPassword;
          data.port = message.masterTcpPort;
          data.lastPingTimestamp = message.masterPing;
          data.master = true;
          knownServers.put(message.masterIdentity, data);
          discoveryListener.nodeConnected(data);
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
    disconnectTimer = new TimerTask() {
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
    };
    taskScheduler.scheduleOnce(disconnectTimer, discoveryPingIntervalMillis);
  }

  private synchronized void checkIfKnownServersAreAlive() {
    Set<ONodeIdentity> toRemove = new HashSet<>();
    for (Map.Entry<ONodeIdentity, ODiscoveryListener.NodeData> entry : knownServers.entrySet()) {
      if (entry.getValue().lastPingTimestamp < System.currentTimeMillis() - maxInactiveServerTimeMillis) {
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(x -> {
      ODiscoveryListener.NodeData val = knownServers.remove(x);
      discoveryListener.nodeDisconnected(val);
    });
  }






  /* =============== LEADER ELECTION ================= */

  /**
   * init the procedure that sends pings to other servers, ie. that notifies that you are alive
   */
  private void initCheckLeader() {
    checkerTimer = new TimerTask() {
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
    };
    taskScheduler.scheduleOnce(checkerTimer, checkLeaderIntervalMillis);
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
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.config.getNodeIdentity();
    message.term = currentTerm;
    message.dbName = dbName;
    message.lastLogId = lastLogId;
    message.type = OBroadcastMessage.TYPE_START_LEADER_ELECTION;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  private void processReceiveLeaderElected(OBroadcastMessage message, String fromAddr) {
    if (message.term >= leaderStatus.currentTerm) {
      if (!this.config.getNodeIdentity().equals(message.nodeIdentity)) {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
      } else {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.LEADER);
      }

      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.identity = message.nodeIdentity;
      data.master = true;
      data.term = message.term;
      data.address = fromAddr;
      data.connectionUsername = message.connectionUsername;
      data.connectionPassword = message.connectionPassword;
      data.port = message.tcpPort;
      data.lastPingTimestamp = System.currentTimeMillis();

      ODiscoveryListener.NodeData oldEntry = this.knownServers.put(data.getNodeIdentity(), data);
      if (oldEntry == null) {
        discoveryListener.nodeConnected(data);
      }

      discoveryListener.leaderElected(data);
    }
  }

  protected void processReceiveStartElection(OBroadcastMessage message, String fromAddr) {
    if (message.term > leaderStatus.currentTerm && message.term > leaderStatus.lastTermVoted) {
      //vote, but only once per term!
      leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
      leaderStatus.lastTermVoted = message.term;
      sendVote(message.term, message.nodeIdentity);
    }
  }

  private void sendVote(int term, ONodeIdentity toNode) {
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.config.getNodeIdentity();
    message.term = term;
    message.voteForIdentity = toNode;
    message.type = OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  protected void processReceiveVote(OBroadcastMessage message, String fromAddr) {
//    System.out.println("RECEIVE VOTE term " + message.term + " from " + message.nodeName + " to " + message.voteForNode);
    if (leaderStatus.status != OLeaderElectionStateMachine.Status.CANDIDATE) {
      return;
    }
    leaderStatus.receiveVote(message.term, message.nodeIdentity, message.voteForIdentity);
    if (leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER) {
      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.term = leaderStatus.currentTerm;
      data.master = true;
      data.identity = this.config.getNodeIdentity();
      data.lastPingTimestamp = System.currentTimeMillis();
      discoveryListener.leaderElected(data);
      knownServers.put(this.config.getNodeIdentity(), data);
      sendLeaderElected();
    }
  }

  private void resetLeader() {
    knownServers.values().forEach(x -> x.master = false);
  }

  private void sendLeaderElected() {
//    System.out.println("SEND LEADER ELECTED " + nodeName);
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.config.getNodeIdentity();
    message.term = leaderStatus.currentTerm;
    message.tcpPort = getConfig().getTcpPort();
    message.type = OBroadcastMessage.TYPE_LEADER_ELECTED;

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }


  /* =============== NETWORK UTILITIES ================= */

  protected byte[] serializeMessage(OBroadcastMessage message) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    message.write(new DataOutputStream(buffer));
    return encrypt(buffer.toByteArray());
  }

  protected OBroadcastMessage deserializeMessage(byte[] data) throws Exception {
    data = decrypt(data);
    if (data == null) {
      return null;
    }
    OBroadcastMessage message = new OBroadcastMessage();
    message.read(new DataInputStream(new ByteArrayInputStream(data)));
    return message;
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
    DataOutput output = new DataOutputStream(stream);
    output.writeInt(iv.length);
    output.write(iv);
    byte[] cypher = cipher.doFinal(data);
    output.writeInt(cypher.length);
    output.write(cypher);

    return stream.toByteArray();
  }

  private byte[] decrypt(byte[] data) throws Exception {
    if (config.getGroupPassword() == null) {
      return data;
    }
    DataInput input = new DataInputStream(new ByteArrayInputStream(data));
    int ivLength = input.readInt();
    byte[] ivData = new byte[ivLength];
    input.readFully(ivData);
    IvParameterSpec ivSpec = new IvParameterSpec(ivData);
    SecretKeySpec skeySpec = new SecretKeySpec(paddedPassword(config.getGroupPassword()), "AES");

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);

    int length = input.readInt();
    byte[] encrypted = new byte[length];
    input.readFully(encrypted);
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
