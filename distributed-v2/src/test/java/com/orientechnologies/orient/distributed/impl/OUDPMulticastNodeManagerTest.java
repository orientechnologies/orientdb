package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.OMulticastConfguration;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

public class OUDPMulticastNodeManagerTest {

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

  protected ONodeConfiguration createConfiguration(String nodeName, int port) {
    return createConfiguration(nodeName, null, port);
  }

  protected ONodeConfiguration createConfiguration(String nodeName, int port, int[] multicastPorts) {
    return createConfiguration(nodeName, null, port, multicastPorts);
  }

  protected ONodeConfiguration createConfiguration(String nodeName, String password, int port) {
    return createConfiguration(nodeName, password, port, new int[] { 4321, 4322 });
  }

  protected ONodeConfiguration createConfiguration(String nodeName, String password, int port, int[] multicastPorts) {
    return createConfiguration(nodeName, "default", password, port, multicastPorts);
  }

  protected ONodeConfiguration createConfiguration(String nodeName, String groupName, String password, int port,
      int[] multicastPorts) {
    return ONodeConfiguration.builder().setNodeName(nodeName).setGroupName(groupName).setGroupPassword(password).setQuorum(2)
        .setTcpPort(2424)
        .setMulticast(OMulticastConfguration.builder().setPort(port).setIp("235.1.1.1").setDiscoveryPorts(multicastPorts).build())
        .build();
  }

  protected ONodeInternalConfiguration createInternalConfiguration() {
    return new ONodeInternalConfiguration(UUID.randomUUID(), "", "");
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

    ONodeInternalConfiguration internalConfiguration = createInternalConfiguration();

    ONodeConfiguration config1 = createConfiguration("node1", 4321);

    OUDPMulticastNodeManager manager1 = new OUDPMulticastNodeManager(config1, internalConfiguration, discoveryListener1, scheduler);
    manager1.start();
    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);
    ONodeConfiguration config2 = createConfiguration("node2", 4322);

    OUDPMulticastNodeManager manager2 = new OUDPMulticastNodeManager(config2, internalConfiguration, discoveryListener2, scheduler);
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

    ONodeInternalConfiguration internalConfiguration = createInternalConfiguration();

    ONodeConfiguration config1 = createConfiguration("node1", "test", 4321);

    OUDPMulticastNodeManager manager1 = new OUDPMulticastNodeManager(config1, internalConfiguration, discoveryListener1, scheduler);

    manager1.start();
    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);

    ONodeConfiguration config2 = createConfiguration("node2", "test", 4321);

    OUDPMulticastNodeManager manager2 = new OUDPMulticastNodeManager(config2, internalConfiguration, discoveryListener2, scheduler);

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

    ONodeInternalConfiguration internalConfiguration = createInternalConfiguration();

    ONodeConfiguration config1 = createConfiguration("node1", "testTwoGroups_default", null, 4321, new int[] { 4321, 4322, 4323 });

    OUDPMulticastNodeManager manager1 = new OUDPMulticastNodeManager(config1, internalConfiguration, discoveryListener1, scheduler);
    manager1.start();

    ONodeConfiguration configOther = createConfiguration("node1", "testTwoGroups_group2", null, 4323, new int[] { 4321, 4322, 4323 });

    OUDPMulticastNodeManager managerOtherGroup = new OUDPMulticastNodeManager(configOther, internalConfiguration,
        discoveryListenerOther, scheduler);
    managerOtherGroup.start();

    Thread.sleep(2000);

    Assert.assertEquals(1, ((MockDiscoveryListener) discoveryListener1).totalNodes);

    ONodeConfiguration config2 = createConfiguration("node2", "testTwoGroups_default", null, 4322, new int[] { 4321, 4322, 4323 });

    OUDPMulticastNodeManager manager2 = new OUDPMulticastNodeManager(config2, internalConfiguration, discoveryListener2, scheduler);
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

    ONodeConfiguration config1 = createConfiguration("node1", 4321);
    ONodeInternalConfiguration internalConfiguration = createInternalConfiguration();

    OUDPMulticastNodeManager manager1 = new OUDPMulticastNodeManager(config1, internalConfiguration, discoveryListener1, scheduler);
    manager1.start();
    Thread.sleep(2000);

    Assert.assertNotEquals(OLeaderElectionStateMachine.Status.LEADER, manager1.leaderStatus.status);
    for (ODiscoveryListener.NodeData value : manager1.knownServers.values()) {
      Assert.assertFalse(value.master);
    }

    ONodeConfiguration config2 = createConfiguration("node2", 4322);

    OUDPMulticastNodeManager manager2 = new OUDPMulticastNodeManager(config2, internalConfiguration, discoveryListener2, scheduler);
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


