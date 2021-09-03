package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.impl.coordinator.MockOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.Test;

public class OSubmitTransactionBeginTest {

  @Test
  public void testBegin() throws InterruptedException {
    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(),
            new MockOperationLog(),
            new ODistributedLockManagerImpl(),
            new OMockAllocator(),
            null,
            "database");

    ONodeIdentity mOne = new ONodeIdentity("one", "one");
    coordinator.join(mOne);
    coordinator.join(new ONodeIdentity("two", "two"));
    coordinator.join(new ONodeIdentity("three", "three"));

    ArrayList<ORecordOperation> recordOps = new ArrayList<>();
    ORecordOperation op = new ORecordOperation(new ORecordId(10, 10), ORecordOperation.CREATED);
    op.setRecord(new ODocument("aaaa"));
    recordOps.add(op);
    coordinator.submit(
        mOne, new OSessionOperationId(), new OTransactionSubmit(recordOps, new ArrayList<>()));
    /*
    assertTrue(cOne.sentRequest.await(1, TimeUnit.SECONDS));
    assertTrue(cTwo.sentRequest.await(1, TimeUnit.SECONDS));
    assertTrue(cThree.sentRequest.await(1, TimeUnit.SECONDS));
     */
  }

  private class MockDistributedChannel implements ODistributedChannel {
    private CountDownLatch sentRequest = new CountDownLatch(1);

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      sentRequest.countDown();
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {}

    @Override
    public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {}

    @Override
    public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {}

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {}

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {}

    @Override
    public void propagate(OLogId id, ORaftOperation operation) {}

    @Override
    public void ack(OLogId logId) {}

    @Override
    public void confirm(OLogId id) {}

    @Override
    public void send(OOperation fullConfiguration) {}
  }
}
