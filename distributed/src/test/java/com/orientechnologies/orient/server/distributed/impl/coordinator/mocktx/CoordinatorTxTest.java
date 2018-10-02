package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    this.one.create("none", ODatabaseType.MEMORY);
    this.two = OrientDBInternal.distributed("target/two/", OrientDBConfig.defaultConfig()).newOrientDB();
    this.two.create("none", ODatabaseType.MEMORY);
    this.three = OrientDBInternal.distributed("target/three/", OrientDBConfig.defaultConfig()).newOrientDB();
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
    ODistributedExecutor eOne = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), OrientDBInternal.extract(this.one),
        "none");
    ODistributedExecutor eTwo = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), OrientDBInternal.extract(this.two),
        "none");
    ODistributedExecutor eThree = new ODistributedExecutor(Executors.newSingleThreadExecutor(), new MockOperationLog(), OrientDBInternal.extract(this.three),
        "none");

    ODatabaseCoordinator coordinator = new ODatabaseCoordinator(Executors.newSingleThreadExecutor(), new MockOperationLog(),
        null, null);

    MemberChannel cOne = new MemberChannel(eOne, coordinator);
    ODistributedMember mOne = new ODistributedMember("one", null, cOne);
    cOne.member = mOne;
    coordinator.join(mOne);

    MemberChannel cTwo = new MemberChannel(eOne, coordinator);
    ODistributedMember mTwo = new ODistributedMember("two", null, cTwo);
    cTwo.member = mTwo;
    coordinator.join(mTwo);

    MemberChannel cThree = new MemberChannel(eOne, coordinator);
    ODistributedMember mThree = new ODistributedMember("three", null, cThree);
    cThree.member = mThree;
    coordinator.join(mThree);
    OSubmitTx submit = new OSubmitTx();
    coordinator.submit(mOne, new OSessionOperationId(), submit);

    assertTrue(cOne.latch.await(10, TimeUnit.SECONDS));
    assertTrue(submit.firstPhase);
    assertTrue(submit.secondPhase);
    eOne.close();
    eTwo.close();
    eThree.close();
    coordinator.close();
    assertEquals(cOne.callCount.get(), 0);

  }

  /**
   * This mock channel unify the channels in two nodes, in real implementation there would be two different channels on two
   * different nodes that would do the half of this job.
   */
  private static class MemberChannel implements ODistributedChannel {
    public  ODistributedExecutor executor;
    public  ODatabaseCoordinator coordinator;
    public  ODistributedMember   member;
    public  CountDownLatch       latch     = new CountDownLatch(1);
    private AtomicLong           callCount = new AtomicLong(1);

    public MemberChannel(ODistributedExecutor executor, ODatabaseCoordinator coordinator) {
      this.executor = executor;
      this.coordinator = coordinator;
    }

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      // Here in real case should be a network call and this method should be call on the other node
      executor.receive(member, id, nodeRequest);
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
      //This in real case should do a network call on the side of the executor node and this call should be in the coordinator node.
      coordinator.receive(member, id, nodeResponse);
    }

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {

    }

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
      latch.countDown();
      callCount.decrementAndGet();
    }
  }

}
