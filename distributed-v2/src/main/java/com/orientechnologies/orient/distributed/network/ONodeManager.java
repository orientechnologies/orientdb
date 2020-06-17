package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public abstract class ONodeManager {

  protected boolean running = true;

  protected final ODiscoveryListener discoveryListener;
  private Thread messageThread;

  protected volatile Map<ONodeIdentity, ODiscoveryListener.NodeData> knownServers;

  protected final ONodeConfiguration config;
  protected final ONodeInternalConfiguration internalConfiguration;

  private String encryptionAlgorithm = "AES";

  protected final OSchedulerInternal taskScheduler;

  protected OOperationLog opLog;

  protected long discoveryPingIntervalMillis = 500; // TODO configure
  protected long checkLeaderIntervalMillis = 1000; // TODO configure
  /**
   * max time a server can be silent (did not get ping from it) until it is considered inactive, ie.
   * left the network
   */
  protected long maxInactiveServerTimeMillis = 5000;

  protected OLeaderElectionStateMachine leaderStatus;
  private TimerTask discoveryTimer;
  private TimerTask disconnectTimer;
  private TimerTask checkerTimer;

  private Map<String, OLogId> databaseLogIds = Collections.synchronizedMap(new LinkedHashMap<>());
  /** max number of database log IDs that have to be sent with a single ping */
  private int maxDbLogsPerPing = 5;

  public ONodeManager(
      ONodeConfiguration config,
      ONodeInternalConfiguration internalConfiguration,
      int term,
      OSchedulerInternal taskScheduler,
      ODiscoveryListener discoveryListener,
      OOperationLog opLog) {
    this.config = config;
    this.internalConfiguration = internalConfiguration;
    if (config.getGroupName() == null || config.getGroupName().length() == 0) {
      throw new IllegalArgumentException("Invalid group name");
    }

    if (internalConfiguration.getNodeIdentity().getName() == null
        || internalConfiguration.getNodeIdentity().getName().length() == 0) {
      throw new IllegalArgumentException("Invalid node name");
    }
    if (internalConfiguration.getNodeIdentity().getId() == null
        || internalConfiguration.getNodeIdentity().getId().length() == 0) {
      throw new IllegalArgumentException("Invalid node id");
    }
    this.opLog = opLog;
    this.discoveryListener = discoveryListener;
    knownServers = new ConcurrentHashMap<>();
    this.taskScheduler = taskScheduler;
    leaderStatus = new OLeaderElectionStateMachine();
    leaderStatus.nodeIdentity = internalConfiguration.getNodeIdentity();
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
   * @throws IOException
   */
  protected abstract void sendMessageToGroup(byte[] msg) throws IOException;

  /**
   * receives messages from the network. It is supposed to invoke {@link
   * #processMessage(OBroadcastMessage, String)} for each message received
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
      OLogManager.instance()
          .error(this, "Cannot start distributed node discovery: " + ex.getMessage(), ex);
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
   * inits the procedure that listens to pings from other servers, eg. that discovers other nodes in
   * the network
   *
   * @throws IOException
   */
  protected void initReceiveMessages() throws IOException {
    messageThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  receiveMessages();
                } catch (Exception e) {
                  // TODO log exception
                }
              }
            });
    messageThread.setName("OrientDB_DistributedDiscoveryThread");
    messageThread.setDaemon(true);
    messageThread.start();
  }

  protected synchronized void processMessage(OBroadcastMessage message, String fromAddr) {
    //    System.out.println(
    //        "MSG toNode: " + this.config.getNodeName() + " fromNode: " +
    // message.getNodeIdentity().getName() + " role: " + message.role + " term: " + message.term
    //            + " type: " + message.type + " master: " + message.masterIdentity + " masterTerm:
    // " + message.masterTerm+" masterPing: "+message.masterPing);
    switch (message.type) {
      case OBroadcastMessage.TYPE_PING:
        //      System.out.println("" + nodeName + " - RECEIVE PING FROM " + message.nodeName);
        processReceivePing(message, fromAddr);
        break;
      case OBroadcastMessage.TYPE_START_LEADER_ELECTION:
        //      System.out.println("" + this.config.getNodeName() + " - RECEIVE START ELECTION FROM
        // " + message.getNodeIdentity().getName());
        processReceiveStartElection(message, fromAddr);
        break;
      case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
        //      System.out.println("" + this.config.getNodeName() + " - RECEIVE VOTE LEADER for " +
        // message.voteForIdentity.getName() + " FROM " + message.getNodeIdentity().getName());
        processReceiveVote(message, fromAddr);
        break;
      case OBroadcastMessage.TYPE_LEADER_ELECTED:
        //      System.out.println("" + this.config.getNodeName() + " - RECEIVE LEADER ELECTED FROM
        // " + message.getNodeIdentity().getName());
        processReceiveLeaderElected(message, fromAddr);
        break;
    }
  }

  /* =============== PING ================= */

  /** init the procedure that sends pings to other servers, ie. that notifies that you are alive */
  protected void initDiscoveryPing() {
    discoveryTimer =
        new TimerTask() {
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
    processReceivePing(ping, getLocalAddress());
    sendMessageToGroup(msg);
  }

  protected abstract String getLocalAddress();

  protected synchronized OBroadcastMessage generatePingMessage() {
    // nodeData
    OBroadcastMessage message = new OBroadcastMessage();
    message.type = OBroadcastMessage.TYPE_PING;
    message.nodeIdentity = this.internalConfiguration.getNodeIdentity();
    message.group = this.config.getGroupName();
    message.term = leaderStatus.currentTerm;
    if (leaderStatus.getStatus() == OLeaderElectionStateMachine.Status.LEADER) {
      message.role = OBroadcastMessage.ROLE_COORDINATOR;
    } else {
      message.role = OBroadcastMessage.ROLE_REPLICA;
    }

    message.connectionUsername = internalConfiguration.getConnectionUsername();
    message.connectionPassword = internalConfiguration.getConnectionPassword();
    message.tcpPort = config.getTcpPort();
    // masterData
    ODiscoveryListener.NodeData master =
        this.knownServers.values().stream().filter(x -> x.leader).findFirst().orElse(null);
    if (master != null) {
      message.leaderTerm = master.term;
      message.leaderAddress = master.address;
      message.leaderTcpPort = master.port;
      message.leaderConnectionUsername = master.connectionUsername;
      message.leaderConnectionPassword = master.connectionPassword;
      //      System.out.println("Sending ping from " + getConfig().getNodeName() + " for " +
      // master.getNodeIdentity().getName() + " to " + master.lastPingTimestamp);
      message.leaderPing = master.lastPingTimestamp;
      message.leaderIdentity = master.getNodeIdentity();
    }

    // DATABASE PINGS

    Iterator<Map.Entry<String, OLogId>> dbLogsIgterator = databaseLogIds.entrySet().iterator();
    int sent = 0;
    message.databasePings = new ArrayList<>();
    while (dbLogsIgterator.hasNext() && sent < maxDbLogsPerPing) {
      try {
        Map.Entry<String, OLogId> next = dbLogsIgterator.next();
        message.databasePings.add(
            new OBroadcastMessage.DatabasePing(next.getKey(), next.getValue()));
        sent++;
      } catch (Exception e) {
        // ignore
      }
    }

    return message;
  }

  protected void processReceivePing(OBroadcastMessage message, String fromAddr) {
    //    System.out.println("Ping on " + this.config.getNodeName() + " from " +
    // message.getNodeIdentity().getName() + " at " + System.currentTimeMillis());
    synchronized (this) {
      if (leaderStatus.currentTerm > message.term) {
        return;
      }
      boolean wasLeader = false;
      ODiscoveryListener.NodeData data = knownServers.get(message.getNodeIdentity());
      if (data == null) {
        data = message.toNodeData();
        // TODO: this should be removed and should be get from the message
        data.address = fromAddr;
        knownServers.put(message.getNodeIdentity(), data);
        discoveryListener.nodeConnected(data);
      } else if (data.leader) {
        wasLeader = true;
      }
      data.lastPingTimestamp = System.currentTimeMillis();
      //      System.out.println("Setting ping on " + getConfig().getNodeName() + " for " +
      // data.getNodeIdentity().getName() + " to " + data.lastPingTimestamp + " entry " + data);
      if (data.term < message.term) {
        data.term = message.term;
        if (message.role == OBroadcastMessage.ROLE_COORDINATOR) {
          resetLeader();
          data.leader = true;
        } else {
          data.leader = false;
        }
        if (leaderStatus.currentTerm < message.term) {
          leaderStatus.changeTerm(message.term);
          if (this.internalConfiguration.getNodeIdentity().equals(message.getNodeIdentity())) {
            leaderStatus.setStatus(OLeaderElectionStateMachine.Status.LEADER);
            opLog.setLeader(true, message.term);
          }
        }
      } else if (data.term == message.term && message.role == OBroadcastMessage.ROLE_COORDINATOR) {
        resetLeader();
        data.leader = true;
        if (!message.getNodeIdentity().equals(this.internalConfiguration.getNodeIdentity())) {
          leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
          opLog.setLeader(false, message.term);
        }
      }
      if (data.leader && !wasLeader) {
        discoveryListener.leaderElected(data);
      }

      // Master info
      if (message.leaderIdentity != null
          && message.leaderTerm >= this.leaderStatus.currentTerm
          && message.leaderPing + maxInactiveServerTimeMillis > System.currentTimeMillis()) {
        data = knownServers.get(message.leaderIdentity);

        if (data == null) {
          data = new ODiscoveryListener.NodeData();
          data.identity = message.leaderIdentity;
          data.term = message.leaderTerm;
          data.address = message.leaderAddress;
          data.connectionUsername = message.leaderConnectionUsername;
          data.connectionPassword = message.leaderConnectionPassword;
          data.port = message.leaderTcpPort;
          //          System.out.println("Setting ping for leader " + data.leader + " to " +
          // message.leaderPing);
          data.lastPingTimestamp = Math.max(data.lastPingTimestamp, message.leaderPing);
          data.leader = true;
          knownServers.put(message.leaderIdentity, data);
          discoveryListener.nodeConnected(data);
          discoveryListener.leaderElected(data);
        }
      }
      if (message.databasePings != null) {
        for (OBroadcastMessage.DatabasePing databasePing : message.databasePings) {
          discoveryListener.lastDbOperation(
              data.getNodeIdentity(), databasePing.databaseName, databasePing.logId);
        }
      }
    }
  }

  /* =============== CHECK SERVERS ALIVE ================= */

  /**
   * inits the procedure that checks if a server is no longer available, ie. if he did not ping for
   * a long time
   */
  protected void initCheckDisconnect() {
    disconnectTimer =
        new TimerTask() {
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
    //    System.out.println("Checking alive servers on " + config.getNodeName());
    Set<ONodeIdentity> toRemove = new HashSet<>();
    for (Map.Entry<ONodeIdentity, ODiscoveryListener.NodeData> entry : knownServers.entrySet()) {
      if (entry.getValue().lastPingTimestamp
          < System.currentTimeMillis() - maxInactiveServerTimeMillis) {
        //        System.out.println("Dead server  " + entry.getKey() + " on " +
        // config.getNodeName() + " entry " + entry.getValue());
        toRemove.add(entry.getKey());
      }
    }
    //    if (toRemove.size() > 0) {
    //      System.out.println(toRemove);
    //    }
    toRemove.forEach(
        x -> {
          ODiscoveryListener.NodeData val = knownServers.remove(x);
          discoveryListener.nodeDisconnected(val);
        });
  }

  /* =============== DATABASE PING INFO ================= */

  public void notifyLastDbOperation(String database, OLogId leaderLastValid) {
    databaseLogIds.put(database, leaderLastValid);
  }

  /* =============== LEADER ELECTION ================= */

  /** init the procedure that sends pings to other servers, ie. that notifies that you are alive */
  private void initCheckLeader() {
    checkerTimer =
        new TimerTask() {
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
      if (leaderStatus.getStatus() == OLeaderElectionStateMachine.Status.CANDIDATE) {
        leaderStatus.resetLeaderElection();
      }
      if (knownServers.size() < leaderStatus.quorum) {
        // no reason to do a leader election in this condition
        return;
      }

      for (ODiscoveryListener.NodeData node : knownServers.values()) {
        if (node.leader && node.term >= leaderStatus.currentTerm) {
          return; // master found
        }
      }
    }

    //    try {
    final ONodeManager sync = this;
    taskScheduler.scheduleOnce(
        new TimerTask() {
          @Override
          public void run() {
            synchronized (sync) {
              for (ODiscoveryListener.NodeData node : knownServers.values()) {
                if (node.leader && node.term >= leaderStatus.currentTerm) {
                  return; // master found
                }
              }

              if (leaderStatus.getStatus() == OLeaderElectionStateMachine.Status.FOLLOWER) {
                leaderStatus.startElection();
                sendStartElection(
                    leaderStatus.currentTerm,
                    null,
                    opLog == null ? 0 : opLog.lastPersistentLog().getId());
              }
            }
          }
        },
        (int) (Math.random() * 2000));
    //      Thread.sleep((int) (Math.random() * 2000));
    //    } catch (InterruptedException e) {
    //    }

    //    synchronized (this) {
    //      for (ODiscoveryListener.NodeData node : knownServers.values()) {
    //        if (node.leader && node.term >= leaderStatus.currentTerm) {
    //          return; //master found
    //        }
    //      }
    //
    //      if (leaderStatus.status == OLeaderElectionStateMachine.Status.FOLLOWER) {
    //        leaderStatus.startElection();
    //        sendStartElection(leaderStatus.currentTerm, null, opLog == null ? 0 :
    // opLog.lastPersistentLog().getId());
    //      }
    //    }
  }

  protected void sendStartElection(int currentTerm, String dbName, long lastLogId) {
    //    System.out.println("" + this.config.getNodeName() + " * START ELECTION term " +
    // currentTerm + " node ");
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.internalConfiguration.getNodeIdentity();
    message.term = currentTerm;
    message.dbName = dbName;
    message.lastLogId = lastLogId;
    message.type = OBroadcastMessage.TYPE_START_LEADER_ELECTION;
    message.connectionUsername = this.getInternalConfiguration().getConnectionUsername();
    message.connectionPassword = this.getInternalConfiguration().getConnectionPassword();
    message.tcpPort = getConfig().getTcpPort();

    try {
      byte[] msg = serializeMessage(message);
      // vote for yourself first
      leaderStatus.lastTermVoted = message.term;
      leaderStatus.receiveVote(message.term, message.nodeIdentity, message.voteForIdentity);
      sendMessageToGroup(msg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void processReceiveLeaderElected(OBroadcastMessage message, String fromAddr) {
    if (message.term >= leaderStatus.currentTerm) {
      if (!this.internalConfiguration.getNodeIdentity().equals(message.nodeIdentity)) {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
        opLog.setLeader(false, message.term);
      } else {
        leaderStatus.setStatus(OLeaderElectionStateMachine.Status.LEADER);
        opLog.setLeader(true, message.term);
      }
      this.leaderStatus.setCurrentTerm(message.term);

      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.identity = message.nodeIdentity;
      data.leader = true;
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
    if (message.term > leaderStatus.currentTerm
        && message.term > leaderStatus.lastTermVoted
        && (opLog == null || message.lastLogId >= opLog.lastPersistentLog().getId())) {
      // vote, but only once per term!
      leaderStatus.setStatus(OLeaderElectionStateMachine.Status.FOLLOWER);
      leaderStatus.lastTermVoted = message.term;
      sendVote(message.term, message.nodeIdentity);
      //      System.out.println(this.config.getNodeName() + " votes for " +
      // message.nodeIdentity.getName());
    }
    //    else {
    //      System.out.println(this.config.getNodeName() + " does not vote for " +
    // message.nodeIdentity.getName());
    //    }
  }

  private void sendVote(int term, ONodeIdentity toNode) {
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.internalConfiguration.getNodeIdentity();
    message.term = term;
    message.voteForIdentity = toNode;
    message.type = OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION;
    message.lastLogId = opLog == null ? 0 : opLog.lastPersistentLog().getId();
    message.connectionUsername = this.getInternalConfiguration().getConnectionUsername();
    message.connectionPassword = this.getInternalConfiguration().getConnectionPassword();
    message.tcpPort = getConfig().getTcpPort();

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  protected void processReceiveVote(OBroadcastMessage message, String fromAddr) {
    //    System.out.println("RECEIVE VOTE term " + message.term + " from " +
    // message.getNodeIdentity().getName() + " to " + message.getNodeIdentity().getName());
    if (leaderStatus.getStatus() != OLeaderElectionStateMachine.Status.CANDIDATE) {
      return;
    }
    leaderStatus.receiveVote(message.term, message.nodeIdentity, message.voteForIdentity);
    if (leaderStatus.getStatus() == OLeaderElectionStateMachine.Status.LEADER) {
      resetLeader();
      ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
      data.term = leaderStatus.currentTerm;
      data.leader = true;
      data.identity = this.internalConfiguration.getNodeIdentity();
      data.lastPingTimestamp = System.currentTimeMillis();
      data.connectionUsername = this.getInternalConfiguration().getConnectionUsername();
      data.connectionPassword = this.getInternalConfiguration().getConnectionPassword();
      data.port = this.getConfig().getTcpPort();
      discoveryListener.leaderElected(data);
      knownServers.put(this.internalConfiguration.getNodeIdentity(), data);
      sendLeaderElected();
      opLog.setLeader(true, message.term);
    }
  }

  private void resetLeader() {
    knownServers.values().forEach(x -> x.leader = false);
  }

  private void sendLeaderElected() {
    //    System.out.println("SEND LEADER ELECTED " + nodeName);
    OBroadcastMessage message = new OBroadcastMessage();
    message.group = this.config.getGroupName();
    message.nodeIdentity = this.internalConfiguration.getNodeIdentity();
    message.term = leaderStatus.currentTerm;
    message.tcpPort = getConfig().getTcpPort();
    message.type = OBroadcastMessage.TYPE_LEADER_ELECTED;
    message.connectionUsername = this.internalConfiguration.getConnectionUsername();
    message.connectionPassword = this.internalConfiguration.getConnectionPassword();
    // TODO: Double check if the leader is itself
    message.leaderConnectionUsername = this.internalConfiguration.getConnectionUsername();
    message.leaderConnectionPassword = this.internalConfiguration.getConnectionPassword();

    try {
      byte[] msg = serializeMessage(message);
      sendMessageToGroup(msg);
    } catch (Exception e) {

    }
  }

  /* =============== NETWORK UTILITIES ================= */

  protected byte[] serializeMessage(OBroadcastMessage message) throws Exception {
    validateMessage(message);
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

  private void validateMessage(OBroadcastMessage message) {
    assert message.type >= 0 && message.type <= 5;
    assert message.nodeIdentity != null;
    assert message.nodeIdentity.getId() != null;
    assert message.nodeIdentity.getName() != null;

    assert message.group != null;
    assert message.term >= 0;
    assert message.role >= 0 && message.role <= 2;
    assert message.connectionUsername != null;
    assert message.connectionPassword != null;

    // for ping
    assert message.tcpPort > 0;

    // for leader election
    if (message.type == OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION) {
      assert message.voteForIdentity != null;
      assert message.voteForIdentity.getId() != null;
    }
    // assert message.dbName != null;
    // assert message.lastLogId >= 0;

    // MASTER INFO
    if (message.leaderIdentity != null) {
      assert message.leaderIdentity != null;
      assert message.leaderIdentity.getId() != null;
      assert message.leaderTerm >= 0;
      assert message.leaderAddress != null;
      assert message.leaderTcpPort > 0;
      assert message.leaderConnectionUsername != null;
      assert message.leaderConnectionPassword != null;
      assert message.leaderPing > 0;
    }
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
      pwd = pwd.substring(0, 16);
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

  public ONodeInternalConfiguration getInternalConfiguration() {
    return internalConfiguration;
  }
}
