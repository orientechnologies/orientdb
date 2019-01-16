package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class OMulticastNodeDiscoveryTaskTestIT {

  class MockDiscoveryListener implements ODiscoveryListener {

    int totalNodes = 0;

    @Override
    public synchronized void nodeJoined(NodeData data) {
      totalNodes++;
    }

    @Override
    public synchronized void nodeLeft(NodeData data) {
      totalNodes--;
    }
  }



  @Test
  public void testMasterElection() throws InterruptedException {
    for (int i = 0; i < 5; i++) {
      testMasterElectionWith(3, 2);
      testMasterElectionWith(5, 3);
      testMasterElectionWith(5, 5);
      testMasterElectionWith(10, 6);
    }
  }

  protected void testMasterElectionWith(int nNodes, int quorum) throws InterruptedException {

    OSchedulerInternal scheduler = new OSchedulerInternal() {
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

    Map<String, OMulticastNodeDiscoveryManager> nodes = new LinkedHashMap<>();
    for (int i = 0; i < nNodes; i++) {
      String nodeName = "node" + i;

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      OMulticastNodeDiscoveryManager node = new OMulticastNodeDiscoveryManager("default", nodeName, quorum, discoveryListener,
          4321 + i, "235.1.1.1", multicastPorts, scheduler);
      node.start();
      nodes.put(nodeName, node);

    }

    Thread.sleep(10000);

    String lastMaster = null;
    for (OMulticastNodeDiscoveryManager node : nodes.values()) {
      int numOfMasters = 0;
      for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
        if (value.master) {
          numOfMasters++;
          if (lastMaster == null) {
            lastMaster = value.name;
          } else {
            Assert.assertEquals(lastMaster, value.name);
          }
        }
      }
      Assert.assertEquals(1, numOfMasters);
    }

    for (int i = 0; i < nNodes - quorum; i++) {

      String leader = nodes.values().stream().filter(x -> x.leaderStatus.status == OLeaderElectionStateMachine.Status.LEADER)
          .map(x -> x.nodeName).findFirst().orElse(null);
      Assert.assertNotNull(leader);
      nodes.remove(leader).stop();

      Thread.sleep(15000);

      lastMaster = null;
      for (OMulticastNodeDiscoveryManager node : nodes.values()) {
        int numOfMasters = 0;
        for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
          if (value.master) {
            numOfMasters++;
            if (lastMaster == null) {
              lastMaster = value.name;
            } else {
              Assert.assertEquals(lastMaster, value.name);
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
    for (int i = 0; i < 5; i++) {
      testJoinAfterMasterElection(3, 2);
      testJoinAfterMasterElection(5, 3);
      testJoinAfterMasterElection(10, 6);
    }
  }

  protected void testJoinAfterMasterElection(int nNodes, int quorum) throws InterruptedException {

    OSchedulerInternal scheduler = new OSchedulerInternal() {
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

    Map<String, OMulticastNodeDiscoveryManager> nodes = new LinkedHashMap<>();
    for (int i = 0; i < quorum; i++) {
      String nodeName = "node" + i;

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      OMulticastNodeDiscoveryManager node = new OMulticastNodeDiscoveryManager("default", nodeName, quorum, discoveryListener,
          4321 + i, "235.1.1.1", multicastPorts, scheduler);
      node.start();
      nodes.put(nodeName, node);

    }

    Thread.sleep(10000);

    String lastMaster = null;
    for (OMulticastNodeDiscoveryManager node : nodes.values()) {
      int numOfMasters = 0;
      for (ODiscoveryListener.NodeData value : node.knownServers.values()) {
        if (value.master) {
          numOfMasters++;
          if (lastMaster == null) {
            lastMaster = value.name;
          } else {
            Assert.assertEquals(lastMaster, value.name);
          }
        }
      }
      Assert.assertEquals(1, numOfMasters);
    }

    for (int i = 0; i < nNodes - quorum; i++) {
      String nodeName = "node" + (i + quorum);

      ODiscoveryListener discoveryListener = new MockDiscoveryListener();
      OMulticastNodeDiscoveryManager node = new OMulticastNodeDiscoveryManager("default", nodeName, quorum, discoveryListener,
          4321 + i + quorum, "235.1.1.1", multicastPorts, scheduler);
      node.start();
      nodes.put(nodeName, node);

      Thread.sleep(6000);

      lastMaster = null;
      for (OMulticastNodeDiscoveryManager node_ : nodes.values()) {
        int numOfMasters = 0;
        for (ODiscoveryListener.NodeData value : node_.knownServers.values()) {
          if (value.master) {
            numOfMasters++;
            if (lastMaster == null) {
              lastMaster = value.name;
            } else {
              Assert.assertEquals(lastMaster, value.name);
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


