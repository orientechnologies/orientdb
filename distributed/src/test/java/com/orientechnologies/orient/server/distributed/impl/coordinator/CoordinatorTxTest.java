package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CoordinatorTxTest {

  private OrientDB one;
  private OrientDB two;
  private OrientDB three;

  @Before
  public void before() {
    this.one = OrientDBInternal.distributed("target/one/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.two = OrientDBInternal.distributed("target/two/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.three = OrientDBInternal.distributed("target/three/", OrientDBConfig.defaultConfig()).newOrientDB();
  }

  @Test
  @Ignore
  public void testTxCoordinator() throws InterruptedException {
    ExecutorSender sender = new ExecutorSender();
    ODistributedExecutor eOne = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), sender,
        this.one);
    ODistributedExecutor eTwo = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), sender,
        this.one);
    ODistributedExecutor eThree = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), sender,
        this.one);

    CoordinatorSender coordinatorSender = new CoordinatorSender();
    coordinatorSender.executors.put("one", eOne);
    coordinatorSender.executors.put("two", eTwo);
    coordinatorSender.executors.put("three", eThree);
    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), new MockOperationLog(),
        coordinatorSender);
    coordinator.join("one");
    coordinator.join("two");
    coordinator.join("three");

    sender.coordinator = coordinator;
    coordinator.submit(new OSubmitTx());

    assertTrue(coordinatorSender.latch.await(10, TimeUnit.SECONDS));

    eOne.close();
    eTwo.close();
    eThree.close();
    coordinator.close();
    assertEquals(coordinatorSender.callCount.get(), 0);

  }

  private static class CoordinatorSender implements OSender {
    public  Map<String, ODistributedExecutor> executors = new HashMap<>();
    public  CountDownLatch                    latch     = new CountDownLatch(1);
    private AtomicLong                        callCount = new AtomicLong(1);

    @Override
    public void sendTo(String node, OLogId id, ONodeMessage request) {
      executors.get(node).receive(node, id, (ONodeRequest) request);
    }

    @Override
    public void sendResponse(String node, OSubmitResponse response) {
      latch.countDown();
      callCount.decrementAndGet();
    }
  }

  private static class ExecutorSender implements OSender {
    public ODistributedCoordinator coordinator;

    @Override
    public void sendTo(String node, OLogId id, ONodeMessage request) {
      coordinator.receive(id, (ONodeResponse) request);
    }

    @Override
    public void sendResponse(String node, OSubmitResponse response) {

    }
  }
}
