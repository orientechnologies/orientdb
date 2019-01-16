package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class OMulticastNodeDiscoveryTaskTest {

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
  public void test() throws InterruptedException {
    ODiscoveryListener discoveryListener1 = new MockDiscoveryListener();
    ODiscoveryListener discoveryListener2 = new MockDiscoveryListener();
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

    OMulticastNodeDiscoveryManager manager1 = new OMulticastNodeDiscoveryManager("default", "node1", 2, discoveryListener1, 4321,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager1.start();
    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);

    OMulticastNodeDiscoveryManager manager2 = new OMulticastNodeDiscoveryManager("default", "node2", 2, discoveryListener2, 4322,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager2.start();
    Thread.sleep(2000);

    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener2).totalNodes);

    manager2.stop();

    Thread.sleep(15000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    manager1.stop();
    Thread.sleep(2000);

  }

  @Test
  public void testEncrypted() throws InterruptedException {
    ODiscoveryListener discoveryListener1 = new MockDiscoveryListener();
    ODiscoveryListener discoveryListener2 = new MockDiscoveryListener();
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

    OMulticastNodeDiscoveryManager manager1 = new OMulticastNodeDiscoveryManager("default", "node1", 2, discoveryListener1, 4321,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager1.setGroupPassword("test");
    manager1.start();
    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);

    OMulticastNodeDiscoveryManager manager2 = new OMulticastNodeDiscoveryManager("default", "node2", 2, discoveryListener2, 4322,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager2.setGroupPassword("test");
    manager2.start();
    Thread.sleep(2000);

    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener2).totalNodes);

    manager2.stop();

    Thread.sleep(15000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    manager1.stop();
    Thread.sleep(5000);

  }

  @Test
  public void testTwoGroups() throws InterruptedException {
    ODiscoveryListener discoveryListener1 = new MockDiscoveryListener();
    ODiscoveryListener discoveryListener2 = new MockDiscoveryListener();
    ODiscoveryListener discoveryListenerOther = new MockDiscoveryListener();
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

    OMulticastNodeDiscoveryManager manager1 = new OMulticastNodeDiscoveryManager("default", "node1", 2, discoveryListener1, 4321,
        "235.1.1.1", new int[] { 4321, 4322, 4323 }, scheduler);
    manager1.start();
    OMulticastNodeDiscoveryManager managerOtherGroup = new OMulticastNodeDiscoveryManager("group2", "node1", 2,
        discoveryListenerOther, 4323, "235.1.1.1", new int[] { 4321, 4322, 4323 }, scheduler);
    managerOtherGroup.start();

    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);

    OMulticastNodeDiscoveryManager manager2 = new OMulticastNodeDiscoveryManager("default", "node2", 2, discoveryListener2, 4322,
        "235.1.1.1", new int[] { 4321, 4322, 4323 }, scheduler);
    manager2.start();
    Thread.sleep(2000);

    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    Assert.assertEquals(2, ((MockDiscoveryListener) discoveryListener2).totalNodes);

    manager2.stop();

    Thread.sleep(15000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    manager1.stop();
    Thread.sleep(2000);
    managerOtherGroup.stop();

  }

  @Test
  public void testMasterElectionWithTwo() throws InterruptedException {
    ODiscoveryListener discoveryListener1 = new MockDiscoveryListener();
    ODiscoveryListener discoveryListener2 = new MockDiscoveryListener();
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

    OMulticastNodeDiscoveryManager manager1 = new OMulticastNodeDiscoveryManager("default", "node1", 2, discoveryListener1, 4321,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager1.start();
    Thread.sleep(2000);

    Assert.assertNotEquals(OLeaderElectionStateMachine.Status.LEADER, manager1.leaderStatus.status);
    for (ODiscoveryListener.NodeData value : manager1.knownServers.values()) {
      Assert.assertFalse(value.master);
    }

    OMulticastNodeDiscoveryManager manager2 = new OMulticastNodeDiscoveryManager("default", "node2", 2, discoveryListener2, 4322,
        "235.1.1.1", new int[] { 4321, 4322 }, scheduler);
    manager2.start();
    Thread.sleep(10000);

    int numOfMasters = 0;
    for (ODiscoveryListener.NodeData value : manager1.knownServers.values()) {
      if (value.master) {
        numOfMasters++;
      }
    }
    Assert.assertEquals(1, numOfMasters);

    manager2.stop();
    manager1.stop();
    Thread.sleep(2000);

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
          System.out.println("For node " + node.nodeName + " master is " + value.name);
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
            System.out.println("For node " + node.nodeName + " master is " + value.name);
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
          System.out.println("For node " + node.nodeName + " master is " + value.name);
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
      System.out.println("STARTING NODE " + nodeName);
      node.start();
      nodes.put(nodeName, node);

      Thread.sleep(6000);

      lastMaster = null;
      for (OMulticastNodeDiscoveryManager node_ : nodes.values()) {
        int numOfMasters = 0;
        for (ODiscoveryListener.NodeData value : node_.knownServers.values()) {
          if (value.master) {
            System.out.println("For node " + node_.nodeName + " master is " + value.name);
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


