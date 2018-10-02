package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class OSubmitTransactionBeginTest {

  @Test
  public void testBegin() throws InterruptedException {
    ODatabaseCoordinator coordinator = new ODatabaseCoordinator(Executors.newSingleThreadExecutor(), new MockOperationLog(),
        new ODistributedLockManagerImpl(0), new OMockAllocator());

    MockChannel cOne = new MockChannel();
    ODistributedMember mOne = new ODistributedMember("one", null, cOne);
    coordinator.join(mOne);

    MockChannel cTwo = new MockChannel();
    ODistributedMember mTwo = new ODistributedMember("two", null, cTwo);
    coordinator.join(mTwo);

    MockChannel cThree = new MockChannel();
    ODistributedMember mThree = new ODistributedMember("three", null, cThree);
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

  private class MockChannel implements ODistributedChannel {
    private CountDownLatch sentRequest = new CountDownLatch(1);

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      sentRequest.countDown();
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {

    }

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {

    }

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {

    }
  }
}
