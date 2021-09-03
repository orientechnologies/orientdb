package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.OMulticastConfguration;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.coordinator.MockOperationLog;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class OUDPMulticastNodeManagerIT {

  class MockDiscoveryListener implements ODiscoveryListener {

    int totalNodes = 0;

    @Override
    public synchronized void nodeConnected(NodeData data) {
      totalNodes++;
    }

    @Override
    public synchronized void nodeDisconnected(NodeData data) {
      totalNodes--;
    }
  }

  @Test
  public void testMasterElection() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      testMasterElectionWith(3, 2);
      testMasterElectionWith(5, 3);
      testMasterElectionWith(5, 5);
      //      testMasterElectionWith(10, 6);
    }
  }

  protected void testMasterElectionWith(int nNodes, int quorum) throws InterruptedException {

    OSchedulerInternal scheduler =
        new OSchedulerInternal() {
          Timer timer = new Timer(true);

          @Override
          public void schedule(TimerTask task, long delay, long period) {
            timer.schedule(task, delay, period);
          }

          @Override
          public void scheduleOnce(TimerTask task, long delay) {
            timer.schedule(task, delay);
          }
        };

    int[] multicastPorts = new int[nNodes];
    for (int j = 0; j < nNodes; j++) {
      multicastPorts[j] = 4321 + j;
    }

    Map<String, OUDPMulticastNodeManager> nodes = new LinkedHashMap<>();
    for (int i = 0; i < nNodes; i++) {
      String nodeName = "node" + i;
      int port = 4321 + i;

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      ONodeConfiguration config =
          ONodeConfiguration.builder()
              .setNodeName(nodeName)
              .setGroupName("testMasterElectionWith_default_" + nNodes + "_" + quorum)
              .setTcpPort(port)
              .setQuorum(quorum)
              .setMulticast(
                  OMulticastConfguration.builder()
                      .setEnabled(true)
                      .setPort(port)
                      .setIp("235.1.1.1")
                      .setDiscoveryPorts(multicastPorts)
                      .build())
              .build();

      ONodeInternalConfiguration internalConfiguration =
          new ONodeInternalConfiguration(
              new ONodeIdentity(UUID.randomUUID().toString(), nodeName), "", "");

      OUDPMulticastNodeManager node =
          new OUDPMulticastNodeManager(
              config, internalConfiguration, discoveryListener, scheduler, new MockOperationLog(0));
      node.start();
      nodes.put(nodeName, node);
    }

    Thread.sleep(10000);

    ONodeIdentity lastMaster = null;
    for (OUDPMulticastNodeManager node : nodes.values()) {
      int numOfMasters = 0;
      for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
        if (value.leader) {
          numOfMasters++;
          if (lastMaster == null) {
            lastMaster = value.getNodeIdentity();
          } else {
            Assert.assertEquals(lastMaster, value.getNodeIdentity());
          }
        }
      }
      Assert.assertEquals(1, numOfMasters);
    }

    for (int i = 0; i < nNodes - quorum; i++) {

      String leader =
          nodes.values().stream()
              .filter(x -> x.leaderStatus.getStatus() == OLeaderElectionStateMachine.Status.LEADER)
              .map(x -> x.getInternalConfiguration().getNodeIdentity().getName())
              .findFirst()
              .orElse(null);
      Assert.assertNotNull(leader);
      nodes.remove(leader).stop();

      Thread.sleep(15000);

      lastMaster = null;
      for (OUDPMulticastNodeManager node : nodes.values()) {
        int numOfMasters = 0;
        for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
          if (value.leader) {
            numOfMasters++;
            if (lastMaster == null) {
              lastMaster = value.getNodeIdentity();
            } else {
              Assert.assertEquals(lastMaster, value.getNodeIdentity());
            }
          }
        }
        Assert.assertEquals(1, numOfMasters);
      }
    }

    nodes.values().forEach(x -> x.stop());
  }

  @Test
  public void testJoinAfterMasterElection() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      testJoinAfterMasterElection(3, 2);
      testJoinAfterMasterElection(5, 3);
      testJoinAfterMasterElection(10, 6);
    }
  }

  protected void testJoinAfterMasterElection(int nNodes, int quorum) throws InterruptedException {

    OSchedulerInternal scheduler =
        new OSchedulerInternal() {
          Timer timer = new Timer(true);

          @Override
          public void schedule(TimerTask task, long delay, long period) {
            timer.schedule(task, delay, period);
          }

          @Override
          public void scheduleOnce(TimerTask task, long delay) {
            timer.schedule(task, delay);
          }
        };

    int[] multicastPorts = new int[nNodes];
    for (int j = 0; j < nNodes; j++) {
      multicastPorts[j] = 4321 + j;
    }

    Map<String, OUDPMulticastNodeManager> nodes = new LinkedHashMap<>();
    for (int i = 0; i < quorum; i++) {
      String nodeName = "node" + i;
      int port = 4321 + i;

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      ONodeConfiguration config =
          ONodeConfiguration.builder()
              .setNodeName(nodeName)
              .setGroupName("testJoinAfterMasterElection_default_" + nNodes + "_" + quorum)
              .setTcpPort(port)
              .setQuorum(quorum)
              .setMulticast(
                  OMulticastConfguration.builder()
                      .setEnabled(true)
                      .setPort(port)
                      .setIp("235.1.1.1")
                      .setDiscoveryPorts(multicastPorts)
                      .build())
              .build();

      ONodeInternalConfiguration internalConfiguration =
          new ONodeInternalConfiguration(
              new ONodeIdentity(UUID.randomUUID().toString(), nodeName), "", "");

      OUDPMulticastNodeManager node =
          new OUDPMulticastNodeManager(
              config, internalConfiguration, discoveryListener, scheduler, new MockOperationLog(0));
      node.start();
      nodes.put(nodeName, node);
    }

    Thread.sleep(10000);

    ONodeIdentity lastMaster = null;
    for (OUDPMulticastNodeManager node : nodes.values()) {
      int numOfMasters = 0;
      for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
        if (value.leader) {
          numOfMasters++;
          if (lastMaster == null) {
            lastMaster = value.getNodeIdentity();
          } else {
            Assert.assertEquals(lastMaster, value.getNodeIdentity());
          }
        }
      }
      Assert.assertEquals(1, numOfMasters);
    }

    for (int i = 0; i < nNodes - quorum; i++) {
      String nodeName = "node" + (i + quorum);
      int port = 4321 + i;

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      ONodeConfiguration config =
          ONodeConfiguration.builder()
              .setNodeName(nodeName)
              .setGroupName("testJoinAfterMasterElection_default_" + nNodes + "_" + quorum)
              .setQuorum(quorum)
              .setTcpPort(port)
              .setMulticast(
                  OMulticastConfguration.builder()
                      .setEnabled(true)
                      .setPort(port)
                      .setIp("235.1.1.1")
                      .setDiscoveryPorts(multicastPorts)
                      .build())
              .build();

      ONodeInternalConfiguration internalConfiguration =
          new ONodeInternalConfiguration(
              new ONodeIdentity(UUID.randomUUID().toString(), nodeName), "", "");

      OUDPMulticastNodeManager node =
          new OUDPMulticastNodeManager(
              config, internalConfiguration, discoveryListener, scheduler, new MockOperationLog(0));
      node.start();
      nodes.put(nodeName, node);

      Thread.sleep(6000);

      lastMaster = null;
      for (OUDPMulticastNodeManager node_ : nodes.values()) {
        int numOfMasters = 0;
        for (ODiscoveryListener.NodeData value : node_.knownServers.values()) {
          if (value.leader) {
            numOfMasters++;
            if (lastMaster == null) {
              lastMaster = value.getNodeIdentity();
            } else {
              Assert.assertEquals(lastMaster, value.getNodeIdentity());
            }
          }
        }
        Assert.assertEquals(1, numOfMasters);
      }
    }

    nodes.values().forEach(x -> x.stop());
    Thread.sleep(2000);
  }
}
