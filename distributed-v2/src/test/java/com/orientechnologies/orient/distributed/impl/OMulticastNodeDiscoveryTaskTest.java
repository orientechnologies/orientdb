package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import org.junit.Assert;
import org.junit.Test;

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
}


