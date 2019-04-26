package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class OSubmitTransactionBeginTest {

  @Test
  public void testBegin() throws InterruptedException {
    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), new MockOperationLog(),
        new ODistributedLockManagerImpl(), new OMockAllocator());

    MockDistributedChannel cOne = new MockDistributedChannel();
    ODistributedMember mOne = new ODistributedMember(new ONodeIdentity("one", "one"), null, cOne);
    coordinator.join(mOne);

    MockDistributedChannel cTwo = new MockDistributedChannel();
    ODistributedMember mTwo = new ODistributedMember(new ONodeIdentity("two", "two"), null, cTwo);
    coordinator.join(mTwo);

    MockDistributedChannel cThree = new MockDistributedChannel();
    ODistributedMember mThree = new ODistributedMember(new ONodeIdentity("three", "three"), null, cThree);
    coordinator.join(mThree);

    ArrayList<ORecordOperation> recordOps = new ArrayList<>();
    ORecordOperation op = new ORecordOperation(new ORecordId(10, 10), ORecordOperation.CREATED);
    op.setRecord(new ODocument("aaaa"));
    recordOps.add(op);
    coordinator.submit(mOne, new OSessionOperationId(), new OTransactionSubmit(recordOps, new ArrayList<>(), false));
    assertTrue(cOne.sentRequest.await(1, TimeUnit.SECONDS));
    assertTrue(cTwo.sentRequest.await(1, TimeUnit.SECONDS));
    assertTrue(cThree.sentRequest.await(1, TimeUnit.SECONDS));
  }

  private class MockDistributedChannel implements ODistributedChannel {
    private CountDownLatch sentRequest = new CountDownLatch(1);

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      sentRequest.countDown();
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {

    }

    @Override
    public void sendResponse(OLogId opId, OStructuralNodeResponse response) {

    }

    @Override
    public void sendRequest(OLogId id, OStructuralNodeRequest request) {

    }

    @Override
    public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {

    }

    @Override
    public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {

    }

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {

    }

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {

    }

    @Override
    public void propagate(OLogId id, ORaftOperation operation) {

    }

    @Override
    public void ack(OLogId logId) {

    }

    @Override
    public void confirm(OLogId id) {

    }
  }
}
