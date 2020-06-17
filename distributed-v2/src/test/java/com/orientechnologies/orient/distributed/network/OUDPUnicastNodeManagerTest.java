package com.orientechnologies.orient.distributed.network;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.OSchedulerInternal;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.config.OUDPUnicastConfiguration;
import com.orientechnologies.orient.core.db.config.OUDPUnicastConfigurationBuilder;
import com.orientechnologies.orient.distributed.impl.ONodeInternalConfiguration;
import com.orientechnologies.orient.distributed.impl.coordinator.MockOperationLog;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class OUDPUnicastNodeManagerTest {

  private static class OSchedulerInternalTest implements OSchedulerInternal {

    Timer timer = new Timer(true);

    @Override
    public void schedule(TimerTask task, long delay, long period) {
      timer.schedule(task, delay, period);
    }

    @Override
    public void scheduleOnce(TimerTask task, long delay) {
      timer.schedule(task, delay);
    }
  }

  class MockDiscoveryListener implements ODiscoveryListener {
    public CountDownLatch connects;
    public CountDownLatch disconnects;
    public int totalNodes = 0;

    public MockDiscoveryListener(int countConnect, int countDisconnect) {
      connects = new CountDownLatch(countConnect);
      disconnects = new CountDownLatch(countDisconnect);
    }

    @Override
    public synchronized void nodeConnected(NodeData data) {
      //      System.out.println("Node Connected: " + data.getNodeIdentity().getName());
      totalNodes++;
      connects.countDown();
    }

    @Override
    public synchronized void nodeDisconnected(NodeData data) {
      //      System.out.println("Node Disconnected: " + data.getNodeIdentity().getName());
      //      System.out.println("now:       " + System.currentTimeMillis());
      //      System.out.println("last ping: " + data.lastPingTimestamp);

      totalNodes--;
      disconnects.countDown();
    }
  }

  protected ONodeConfiguration createConfiguration(String nodeName, int port) {
    return createConfiguration(nodeName, null, port);
  }

  protected ONodeConfiguration createConfiguration(String nodeName, int port, int[] ports) {
    return createConfiguration(nodeName, null, port, ports);
  }

  protected ONodeConfiguration createConfiguration(String nodeName, String password, int port) {
    return createConfiguration(nodeName, password, port, new int[] {4321, 4322});
  }

  protected ONodeConfiguration createConfiguration(
      String nodeName, String password, int port, int[] ports) {
    return createConfiguration(nodeName, "default", password, port, ports);
  }

  protected ONodeConfiguration createConfiguration(
      String nodeName, String groupName, String password, int port, int[] unicastPorts) {

    OUDPUnicastConfigurationBuilder unicastConfigBuilder =
        OUDPUnicastConfiguration.builder().setPort(port).setEnabled(true);
    for (int unicastPort : unicastPorts) {
      unicastConfigBuilder.addAddress("localhost", unicastPort);
    }
    ONodeConfiguration config =
        ONodeConfiguration.builder()
            .setNodeName(nodeName)
            .setGroupName(groupName)
            .setGroupPassword(password)
            .setQuorum(2)
            .setTcpPort(2424)
            .setUnicast(unicastConfigBuilder.build())
            .build();
    return config;
  }

  protected ONodeInternalConfiguration createInternalConfiguration(String nodeName) {
    return new ONodeInternalConfiguration(
        new ONodeIdentity(UUID.randomUUID().toString(), nodeName), "", "");
  }

  //  @Ignore
  @Test
  public void test() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 1);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(2, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");

    ONodeConfiguration config1 = createConfiguration("node1", 4321);

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(0));
    manager1.start();
    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");
    ONodeConfiguration config2 = createConfiguration("node2", 4322);

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(0));
    manager2.start();
    assertTrue(discoveryListener2.connects.await(2, TimeUnit.SECONDS));
    Thread.sleep(100);
    Assert.assertEquals(2, discoveryListener1.totalNodes);
    Assert.assertEquals(2, discoveryListener2.totalNodes);

    manager2.stop();

    assertTrue(discoveryListener1.disconnects.await(15, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    manager1.stop();
    Thread.sleep(2000);
  }

  //  @Ignore
  @Test
  public void testEncrypted() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 1);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(2, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");

    ONodeConfiguration config1 = createConfiguration("node1", "test", 4321);

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(0));

    manager1.start();
    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);

    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");
    ONodeConfiguration config2 = createConfiguration("node2", "test", 4322);

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(0));

    manager2.start();

    try {
      assertTrue(discoveryListener2.connects.await(2, TimeUnit.SECONDS));
      Thread.sleep(100);
      Assert.assertEquals(2, discoveryListener1.totalNodes);
      Assert.assertEquals(2, discoveryListener2.totalNodes);

      manager2.stop();

      assertTrue(discoveryListener1.disconnects.await(15, TimeUnit.SECONDS));

      Assert.assertEquals(1, discoveryListener1.totalNodes);
    } finally {
      manager1.stop();
      manager2.stop();
    }
    Thread.sleep(3000);
  }

  @Test
  //  @Ignore
  public void testTwoGroups() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 1);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(2, 0);
    MockDiscoveryListener discoveryListenerOther = new MockDiscoveryListener(0, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");

    ONodeConfiguration config1 =
        createConfiguration(
            "node1", "testTwoGroups_default", null, 4321, new int[] {4321, 4322, 4323});

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(0));
    manager1.start();

    ONodeConfiguration configOther =
        createConfiguration(
            "node1", "testTwoGroups_group2", null, 4323, new int[] {4321, 4322, 4323});

    OUDPUnicastNodeManager managerOtherGroup =
        new OUDPUnicastNodeManager(
            configOther,
            internalConfiguration1,
            discoveryListenerOther,
            scheduler,
            new MockOperationLog(0));
    managerOtherGroup.start();

    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");

    ONodeConfiguration config2 =
        createConfiguration(
            "node2", "testTwoGroups_default", null, 4322, new int[] {4321, 4322, 4323});

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(0));
    manager2.start();
    Thread.sleep(3000);
    assertTrue(discoveryListener2.connects.await(20, TimeUnit.SECONDS));

    Assert.assertEquals(2, discoveryListener1.totalNodes);
    Assert.assertEquals(2, discoveryListener2.totalNodes);

    manager2.stop();

    assertTrue(discoveryListener1.disconnects.await(20, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    manager1.stop();
    Thread.sleep(2000);
    managerOtherGroup.stop();
  }

  @Test
  //  @Ignore
  public void testMasterElectionWithTwo() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 0);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(1, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeConfiguration config1 = createConfiguration("node1", 4321);
    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");
    config1.setGroupName("testMasterElectionWithTwo");
    config1.setGroupPassword("testMasterElectionWithTwo");

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(0));
    manager1.start();
    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertNotEquals(
        OLeaderElectionStateMachine.Status.LEADER, manager1.leaderStatus.getStatus());
    for (ODiscoveryListener.NodeData value : manager1.knownServers.values()) {
      Assert.assertFalse(value.leader);
    }

    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");
    ONodeConfiguration config2 = createConfiguration("node2", 4322);
    config2.setGroupName("testMasterElectionWithTwo");
    config2.setGroupPassword("testMasterElectionWithTwo");

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(0));
    manager2.start();
    assertTrue(discoveryListener2.connects.await(2, TimeUnit.SECONDS));
    Thread.sleep(10000);
    int numOfMasters = 0;
    for (ODiscoveryListener.NodeData value : manager1.knownServers.values()) {
      if (value.leader) {
        numOfMasters++;
      }
      //      System.out.println(value.lastPingTimestamp);
    }
    Assert.assertEquals(1, numOfMasters);

    manager2.stop();
    manager1.stop();
    Thread.sleep(2000);
  }

  @Test
  //  @Ignore
  public void testMasterWithHighestOplog() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 1);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(2, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");

    ONodeConfiguration config1 =
        createConfiguration(
            "node1", "testMasterWithHighestOplog", "testPw", 4321, new int[] {4321, 4322});

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(0));
    manager1.start();
    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");
    ONodeConfiguration config2 =
        createConfiguration(
            "node2", "testMasterWithHighestOplog", "testPw", 4322, new int[] {4321, 4322});

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(10));
    manager2.start();
    assertTrue(discoveryListener2.connects.await(2, TimeUnit.SECONDS));
    Thread.sleep(5000);
    Assert.assertEquals(2, discoveryListener1.totalNodes);
    //    if (discoveryListener2.totalNodes < 2) {
    //      System.out.println(discoveryListener2.totalNodes);
    //    }
    Assert.assertEquals(2, discoveryListener2.totalNodes);

    Assert.assertEquals(
        OLeaderElectionStateMachine.Status.FOLLOWER, manager1.leaderStatus.getStatus());
    Assert.assertEquals(
        OLeaderElectionStateMachine.Status.LEADER, manager2.leaderStatus.getStatus());
    manager2.stop();

    assertTrue(discoveryListener1.disconnects.await(15, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    manager1.stop();
    Thread.sleep(2000);
  }

  @Test
  //  @Ignore
  public void testMasterWithHighestOplog2() throws InterruptedException {
    MockDiscoveryListener discoveryListener1 = new MockDiscoveryListener(1, 1);
    MockDiscoveryListener discoveryListener2 = new MockDiscoveryListener(2, 0);
    OSchedulerInternal scheduler = new OSchedulerInternalTest();

    ONodeInternalConfiguration internalConfiguration1 = createInternalConfiguration("node1");

    ONodeConfiguration config1 =
        createConfiguration(
            "node1",
            "testMasterWithHighestOplog2",
            "testMasterWithHighestOplog2",
            4321,
            new int[] {4321, 4322});

    OUDPUnicastNodeManager manager1 =
        new OUDPUnicastNodeManager(
            config1,
            internalConfiguration1,
            discoveryListener1,
            scheduler,
            new MockOperationLog(10));
    manager1.start();
    assertTrue(discoveryListener1.connects.await(2, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    ONodeInternalConfiguration internalConfiguration2 = createInternalConfiguration("node2");
    ONodeConfiguration config2 =
        createConfiguration(
            "node2",
            "testMasterWithHighestOplog2",
            "testMasterWithHighestOplog2",
            4322,
            new int[] {4321, 4322});

    OUDPUnicastNodeManager manager2 =
        new OUDPUnicastNodeManager(
            config2,
            internalConfiguration2,
            discoveryListener2,
            scheduler,
            new MockOperationLog(0));
    manager2.start();
    assertTrue(discoveryListener2.connects.await(2, TimeUnit.SECONDS));
    Thread.sleep(5000);
    Assert.assertEquals(2, discoveryListener1.totalNodes);
    //    if (discoveryListener2.totalNodes < 2) {
    //      System.out.println(discoveryListener2.totalNodes);
    //    }
    Assert.assertEquals(2, discoveryListener2.totalNodes);

    Assert.assertEquals(
        OLeaderElectionStateMachine.Status.LEADER, manager1.leaderStatus.getStatus());
    Assert.assertEquals(
        OLeaderElectionStateMachine.Status.FOLLOWER, manager2.leaderStatus.getStatus());
    manager2.stop();

    assertTrue(discoveryListener1.disconnects.await(15, TimeUnit.SECONDS));

    Assert.assertEquals(1, discoveryListener1.totalNodes);
    manager1.stop();
    Thread.sleep(2000);
  }
}
