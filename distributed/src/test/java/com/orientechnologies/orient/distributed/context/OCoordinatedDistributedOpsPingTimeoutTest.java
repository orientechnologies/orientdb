package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OCoordinatedDistributedOpsPingTimeoutTest {

  @Test
  public void timeoutTest() throws InterruptedException {
    OFlowSimulator flow = new OFlowSimulator(2);
    var node1 = flow.bootNode();
    var node2 = flow.bootNode();
    var node3 = flow.bootNode();
    assertFalse(flow.checkOffline(10));
    Thread.sleep(5);
    assertTrue(flow.checkOffline(2));
    flow.pingAll(node1);
    flow.pingAll(node2);
    flow.pingAll(node3);
    assertFalse(flow.checkOffline(2));
  }
}
