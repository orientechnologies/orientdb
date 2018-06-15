package com.orientechnologies.orient.server.distributed.impl.coordinator;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ODistributedCoordinatorTest {

  private OOperationLog operationLog = new MockOperationLog();

  private MockOSender sender = new MockOSender();

  @Before
  public void before() {
  }

  @Test
  public void simpleOperationTest() {

    ODistributedCoordinator coordinator = new ODistributedCoordinator(Executors.newSingleThreadExecutor(), operationLog, sender);
    sender.coordinator = coordinator;

    coordinator.submit(new OSubmitRequest() {
      @Override
      public void begin(ODistributedCoordinator coordinator) {
        ONodeRequest nodeRequest = new ONodeRequest() {
        };
        coordinator.logAndCreateContext(this, nodeRequest);
        coordinator.sendAll(nodeRequest);
      }
    });
  }

  private static class MockOSender implements OSender {
    public ODistributedCoordinator coordinator;

    @Override
    public void sendAll(ONodeMessage request) {
      coordinator.receive(new ONodeResponse() {
        @Override
        public OLogId getOperationLogId() {
          return null;
        }
      });
    }

    @Override
    public void sendTo(String node, ONodeMessage request) {

    }
  }

  private static class MockOperationLog implements OOperationLog {
    private AtomicLong sequence = new AtomicLong(0);

    @Override
    public OLogId log(ONodeRequest request) {
      return new OLogId(sequence.incrementAndGet());
    }
  }
}
