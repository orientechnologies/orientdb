package com.orientechnologies.orient.server.distributed.impl.coordinator;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ODistributedCoordinatorTest {

  @Test
  public void simpleOperationTest() throws InterruptedException {
    CountDownLatch responseReceived = new CountDownLatch(1);
    OOperationLog operationLog = new MockOperationLog();

    MockOSender sender = new MockOSender();

    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), operationLog, sender);
    coordinator.join("one");
    sender.coordinator = coordinator;

    coordinator.submit(new OSubmitRequest() {
      @Override
      public void begin(ODistributedCoordinator coordinator) {
        MockNodeRequest nodeRequest = new MockNodeRequest();
        coordinator.sendOperation(this, nodeRequest, (coordinator1, context, response) -> {
          if (context.getResponses().size() == 1) {
            responseReceived.countDown();
          }
        });
      }

    });
    assertTrue(responseReceived.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testTwoPhase() throws InterruptedException {
    CountDownLatch responseReceived = new CountDownLatch(1);
    OOperationLog operationLog = new MockOperationLog();

    MockOSender sender = new MockOSender();

    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), operationLog, sender);
    coordinator.join("one");
    sender.coordinator = coordinator;

    coordinator.submit(new OSubmitRequest() {
      private boolean state = true;

      @Override
      public void begin(ODistributedCoordinator coordinator) {
        MockNodeRequest nodeRequest = new MockNodeRequest();
        coordinator.sendOperation(this, nodeRequest, (coordinator1, context, response) -> {
          if (context.getResponses().size() == 1) {
            coordinator1.sendOperation(this, new ONodeRequest() {
              @Override
              public ONodeResponse execute(String nodeFrom, OLogId opId, ODistributedExecutor executor) {
                return null;
              }
            }, (coordinator2, context1, response1) -> {
              if (context.getResponses().size() == 1) {
                coordinator.reply(new OSubmitResponse() {
                });
                responseReceived.countDown();
              }
            });
          }
        });
      }
    });

    assertTrue(responseReceived.await(1, TimeUnit.SECONDS));

  }

  private static class MockOSender implements OSender {
    public ODistributedCoordinator coordinator;

    @Override
    public void sendTo(String node, OLogId id, ONodeMessage request) {
      coordinator.receive(id, new ONodeResponse() {
      });
    }

    @Override
    public void sendResponse(String node, OSubmitResponse response) {

    }
  }

  private static class MockNodeRequest implements ONodeRequest {

    @Override
    public ONodeResponse execute(String nodeFrom, OLogId opId, ODistributedExecutor executor) {
      return null;
    }
  }
}
