package com.orientechnologies.orient.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.MockOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorTxTest {

  private OrientDB one;
  private OrientDB two;
  private OrientDB three;

  @Before
  public void before() {
    this.one =
        OrientDBInternal.embedded("target/one/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.one.create("none", ODatabaseType.MEMORY);
    this.two =
        OrientDBInternal.embedded("target/two/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.two.create("none", ODatabaseType.MEMORY);
    this.three =
        OrientDBInternal.embedded("target/three/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.three.create("none", ODatabaseType.MEMORY);
  }

  @After
  public void after() {
    one.close();
    two.close();
    three.close();
  }

  @Test
  public void testTxCoordinator() throws InterruptedException {
    MockNetworkCoord network = new MockNetworkCoord();

    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(),
            new MockOperationLog(),
            null,
            null,
            network,
            "database");

    ONodeIdentity mOne = new ONodeIdentity("one", "one");
    ODistributedNetwork networkNode = new MockNetworkNode(mOne, coordinator);
    ODistributedExecutor eOne =
        new ODistributedExecutor(
            Executors.newSingleThreadExecutor(),
            new MockOperationLog(),
            OrientDBInternal.extract(this.one),
            networkNode,
            "none");
    ONodeIdentity mTwo = new ONodeIdentity("two", "two");
    ODistributedNetwork networkNode2 = new MockNetworkNode(mTwo, coordinator);
    ODistributedExecutor eTwo =
        new ODistributedExecutor(
            Executors.newSingleThreadExecutor(),
            new MockOperationLog(),
            OrientDBInternal.extract(this.two),
            networkNode2,
            "none");

    ONodeIdentity mThree = new ONodeIdentity("three", "three");
    ODistributedNetwork networkNode3 = new MockNetworkNode(mThree, coordinator);
    ODistributedExecutor eThree =
        new ODistributedExecutor(
            Executors.newSingleThreadExecutor(),
            new MockOperationLog(),
            OrientDBInternal.extract(this.three),
            networkNode3,
            "none");

    network.coordinator = coordinator;
    network.executor.put(mOne, eOne);
    network.executor.put(mTwo, eTwo);
    network.executor.put(mThree, eThree);
    network.coordinatorId = mOne;

    coordinator.join(mOne);
    coordinator.join(mTwo);
    coordinator.join(mThree);

    OSubmitTx submit = new OSubmitTx();
    coordinator.submit(mOne, new OSessionOperationId(), submit);
    /*
    TODO:
    assertTrue(cOne.latch.await(10, TimeUnit.SECONDS));
    assertTrue(submit.firstPhase);
    assertTrue(submit.secondPhase);
    eOne.close();
    eTwo.close();
    eThree.close();
    coordinator.close();
    assertEquals(cOne.callCount.get(), 0);

     */

  }

  /**
   * This mock channel unify the channels in two nodes, in real implementation there would be two
   * different channels on two different nodes that would do the half of this job.
   */
  private static class MemberChannel implements ODistributedChannel {
    public ODistributedExecutor executor;
    public ODistributedCoordinator coordinator;
    public CountDownLatch latch = new CountDownLatch(1);
    private AtomicLong callCount = new AtomicLong(1);

    public MemberChannel(ODistributedExecutor executor, ODistributedCoordinator coordinator) {
      this.executor = executor;
      this.coordinator = coordinator;
    }

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      // Here in real case should be a network call and this method should be call on the other node
      // executor.receive(member, id, nodeRequest);
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
      // This in real case should do a network call on the side of the executor node and this call
      // should be in the coordinator node.
      // coordinator.receive(member, id, nodeResponse);
    }

    @Override
    public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {}

    @Override
    public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {}

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {}

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
      latch.countDown();
      callCount.decrementAndGet();
    }

    @Override
    public void propagate(OLogId id, ORaftOperation operation) {}

    @Override
    public void ack(OLogId logId) {}

    @Override
    public void confirm(OLogId id) {}

    @Override
    public void send(OOperation fullConfiguration) {}
  }

  private class MockNetworkCoord implements ODistributedNetwork {
    public Map<ONodeIdentity, ODistributedExecutor> executor = new HashMap<>();
    public ODistributedCoordinator coordinator;
    private ONodeIdentity coordinatorId;
    public CountDownLatch latch = new CountDownLatch(1);
    private AtomicLong callCount = new AtomicLong(1);

    @Override
    public void submit(
        ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitRequest request) {}

    @Override
    public void reply(
        ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitResponse response) {}

    @Override
    public void propagate(Collection<ONodeIdentity> to, OLogId id, ORaftOperation operation) {}

    @Override
    public void ack(ONodeIdentity to, OLogId logId) {}

    @Override
    public void confirm(Collection<ONodeIdentity> to, OLogId id) {}

    @Override
    public void submit(
        ONodeIdentity coordinator,
        String database,
        OSessionOperationId operationId,
        OSubmitRequest request) {}

    @Override
    public void replay(
        ONodeIdentity to,
        String database,
        OSessionOperationId operationId,
        OSubmitResponse response) {
      latch.countDown();
      callCount.decrementAndGet();
    }

    @Override
    public void sendResponse(
        ONodeIdentity to, String database, OLogId opId, ONodeResponse response) {}

    @Override
    public void sendRequest(
        Collection<ONodeIdentity> to, String database, OLogId id, ONodeRequest request) {
      for (ONodeIdentity member : to) {
        executor.get(member).receive(coordinatorId, id, request);
      }
    }

    @Override
    public void send(ONodeIdentity identity, OOperation operation) {}

    @Override
    public void sendAll(Collection<ONodeIdentity> members, OOperation operation) {}

    @Override
    public void notifyLastDbOperation(
        ONodeIdentity leader, String database, OLogId leaderLastValid) {}

    @Override
    public void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid) {}
  }

  private class MockNetworkNode implements ODistributedNetwork {
    private ONodeIdentity memberId;
    public ODistributedCoordinator coordinator;

    public MockNetworkNode(ONodeIdentity memberId, ODistributedCoordinator coordinator) {
      this.memberId = memberId;
      this.coordinator = coordinator;
    }

    @Override
    public void submit(
        ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitRequest request) {}

    @Override
    public void reply(
        ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitResponse response) {}

    @Override
    public void propagate(Collection<ONodeIdentity> to, OLogId id, ORaftOperation operation) {}

    @Override
    public void ack(ONodeIdentity to, OLogId logId) {}

    @Override
    public void confirm(Collection<ONodeIdentity> to, OLogId id) {}

    @Override
    public void submit(
        ONodeIdentity coordinator,
        String database,
        OSessionOperationId operationId,
        OSubmitRequest request) {}

    @Override
    public void replay(
        ONodeIdentity to,
        String database,
        OSessionOperationId operationId,
        OSubmitResponse response) {}

    @Override
    public void sendResponse(
        ONodeIdentity to, String database, OLogId opId, ONodeResponse response) {
      coordinator.receive(memberId, opId, response);
    }

    @Override
    public void sendRequest(
        Collection<ONodeIdentity> to, String database, OLogId id, ONodeRequest request) {}

    @Override
    public void send(ONodeIdentity identity, OOperation operation) {}

    @Override
    public void sendAll(Collection<ONodeIdentity> members, OOperation operation) {}

    @Override
    public void notifyLastDbOperation(
        ONodeIdentity leader, String database, OLogId leaderLastValid) {}

    @Override
    public void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid) {}
  }
}
