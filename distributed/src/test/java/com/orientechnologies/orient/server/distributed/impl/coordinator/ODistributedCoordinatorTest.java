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

    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), operationLog);
    MockChannel channel = new MockChannel();
    channel.coordinator = coordinator;
    ODistributedMember one = new ODistributedMember("one", channel);
    coordinator.join(one);

    coordinator.submit(one, new OSubmitRequest() {
      @Override
      public void begin(ODistributedMember member, ODistributedCoordinator coordinator) {
        MockNodeRequest nodeRequest = new MockNodeRequest();
        coordinator.sendOperation(this, nodeRequest, new OResponseHandler() {
          @Override
          public void receive(ODistributedCoordinator coordinator, ORequestContext context, ONodeResponse response) {
            if (context.getResponses().size() == 1) {
              responseReceived.countDown();
            }
          }

          @Override
          public void timeout(ODistributedCoordinator coordinator, ORequestContext context) {

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

    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), operationLog);
    MockChannel channel = new MockChannel();
    channel.coordinator = coordinator;
    channel.reply = responseReceived;
    ODistributedMember one = new ODistributedMember("one", channel);
    coordinator.join(one);

    coordinator.submit(one, new OSubmitRequest() {
      private boolean state = true;

      @Override
      public void begin(ODistributedMember member, ODistributedCoordinator coordinator) {
        MockNodeRequest nodeRequest = new MockNodeRequest();
        coordinator.sendOperation(this, nodeRequest, new OResponseHandler() {
          @Override
          public void receive(ODistributedCoordinator coordinator, ORequestContext context, ONodeResponse response) {
            if (context.getResponses().size() == 1) {
              coordinator.sendOperation(null, new ONodeRequest() {
                @Override
                public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor) {
                  return null;
                }
              }, new OResponseHandler() {
                @Override
                public void receive(ODistributedCoordinator coordinator, ORequestContext context, ONodeResponse response) {
                  if (context.getResponses().size() == 1) {
                    member.reply(new OSubmitResponse() {
                    });
                  }
                }

                @Override
                public void timeout(ODistributedCoordinator coordinator, ORequestContext context) {

                }
              });
            }
          }

          @Override
          public void timeout(ODistributedCoordinator coordinator, ORequestContext context) {

          }
        });
      }
    });

    assertTrue(responseReceived.await(1, TimeUnit.SECONDS));

  }

  private static class MockChannel implements ODistributedChannel {
    public ODistributedCoordinator coordinator;
    public CountDownLatch          reply;

    @Override
    public void sendRequest(OLogId id, ONodeRequest request) {
      coordinator.receive(id, new ONodeResponse() {
      });
    }

    @Override
    public void sendResponse(OLogId id, ONodeResponse nodeResponse) {
      assertTrue(false);
    }

    @Override
    public void reply(OSubmitResponse response) {
      reply.countDown();
    }
  }

  private static class MockNodeRequest implements ONodeRequest {

    @Override
    public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor) {
      return null;
    }
  }
}
