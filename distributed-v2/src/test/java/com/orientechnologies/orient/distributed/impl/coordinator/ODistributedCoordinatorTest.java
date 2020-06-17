package com.orientechnologies.orient.distributed.impl.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ODistributedCoordinatorTest {

  @Test
  public void simpleOperationTest() throws InterruptedException {
    CountDownLatch responseReceived = new CountDownLatch(1);
    OOperationLog operationLog = new MockOperationLog();
    MockNetword network = new MockNetword();
    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(), operationLog, null, null, network, "database");
    network.coordinator = coordinator;
    ONodeIdentity one = new ONodeIdentity("one", "one");
    coordinator.join(one);

    coordinator.submit(
        one,
        new OSessionOperationId(),
        new OSubmitRequest() {
          @Override
          public void begin(
              ONodeIdentity requester,
              OSessionOperationId operationId,
              ODistributedCoordinator coordinator) {
            MockNodeRequest nodeRequest = new MockNodeRequest();
            coordinator.sendOperation(
                this,
                nodeRequest,
                new OResponseHandler() {
                  @Override
                  public boolean receive(
                      ODistributedCoordinator coordinator,
                      ORequestContext context,
                      ONodeIdentity member,
                      ONodeResponse response) {
                    if (context.getResponses().size() == 1) {
                      responseReceived.countDown();
                    }
                    return context.getResponses().size() == context.getInvolvedMembers().size();
                  }

                  @Override
                  public boolean timeout(
                      ODistributedCoordinator coordinator, ORequestContext context) {
                    return true;
                  }
                });
          }

          @Override
          public void serialize(DataOutput output) {}

          @Override
          public void deserialize(DataInput input) {}

          @Override
          public int getRequestType() {
            return 0;
          }
        });
    assertTrue(responseReceived.await(1, TimeUnit.SECONDS));
    coordinator.close();
    assertEquals(0, coordinator.getContexts().size());
  }

  @Test
  public void testTwoPhase() throws InterruptedException {
    CountDownLatch responseReceived = new CountDownLatch(1);
    OOperationLog operationLog = new MockOperationLog();
    MockNetword network = new MockNetword();

    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(), operationLog, null, null, network, "database");
    network.coordinator = coordinator;
    network.reply = responseReceived;
    ONodeIdentity one = new ONodeIdentity("one", "one");
    coordinator.join(one);

    coordinator.submit(
        one,
        new OSessionOperationId(),
        new OSubmitRequest() {
          @Override
          public void begin(
              ONodeIdentity requester,
              OSessionOperationId operationId,
              ODistributedCoordinator coordinator) {
            MockNodeRequest nodeRequest = new MockNodeRequest();
            coordinator.sendOperation(
                this,
                nodeRequest,
                new OResponseHandler() {
                  @Override
                  public boolean receive(
                      ODistributedCoordinator coordinator,
                      ORequestContext context,
                      ONodeIdentity member,
                      ONodeResponse response) {
                    if (context.getResponses().size() == 1) {
                      coordinator.sendOperation(
                          null,
                          new ONodeRequest() {
                            @Override
                            public ONodeResponse execute(
                                ONodeIdentity nodeFrom,
                                OLogId opId,
                                ODistributedExecutor executor,
                                ODatabaseDocumentInternal session) {
                              return null;
                            }

                            @Override
                            public void serialize(DataOutput output) {}

                            @Override
                            public void deserialize(DataInput input) {}

                            @Override
                            public int getRequestType() {
                              return 0;
                            }
                          },
                          new OResponseHandler() {
                            @Override
                            public boolean receive(
                                ODistributedCoordinator coordinator,
                                ORequestContext context,
                                ONodeIdentity member,
                                ONodeResponse response) {
                              if (context.getResponses().size() == 1) {
                                coordinator.reply(
                                    member,
                                    new OSessionOperationId(),
                                    new OSubmitResponse() {
                                      @Override
                                      public void serialize(DataOutput output) throws IOException {}

                                      @Override
                                      public void deserialize(DataInput input) throws IOException {}

                                      @Override
                                      public int getResponseType() {
                                        return 0;
                                      }
                                    });
                              }
                              return context.getResponses().size()
                                  == context.getInvolvedMembers().size();
                            }

                            @Override
                            public boolean timeout(
                                ODistributedCoordinator coordinator, ORequestContext context) {
                              return true;
                            }
                          });
                    }
                    return context.getResponses().size() == context.getInvolvedMembers().size();
                  }

                  @Override
                  public boolean timeout(
                      ODistributedCoordinator coordinator, ORequestContext context) {
                    return true;
                  }
                });
          }

          @Override
          public void serialize(DataOutput output) {}

          @Override
          public void deserialize(DataInput input) {}

          @Override
          public int getRequestType() {
            return 0;
          }
        });

    assertTrue(responseReceived.await(1, TimeUnit.SECONDS));
    coordinator.close();
    assertEquals(0, coordinator.getContexts().size());
  }

  @Test
  public void simpleTimeoutTest() throws InterruptedException {
    CountDownLatch timedOut = new CountDownLatch(1);
    OOperationLog operationLog = new MockOperationLog();
    MockNetword network = new MockNetword();

    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(), operationLog, null, null, network, "database");
    network.coordinator = coordinator;
    ONodeIdentity one = new ONodeIdentity("one", "one");
    coordinator.join(one);

    coordinator.submit(
        one,
        new OSessionOperationId(),
        new OSubmitRequest() {
          @Override
          public void begin(
              ONodeIdentity requester,
              OSessionOperationId operationId,
              ODistributedCoordinator coordinator) {
            MockNodeRequest nodeRequest = new MockNodeRequest();
            coordinator.sendOperation(
                this,
                nodeRequest,
                new OResponseHandler() {
                  @Override
                  public boolean receive(
                      ODistributedCoordinator coordinator,
                      ORequestContext context,
                      ONodeIdentity member,
                      ONodeResponse response) {
                    return false;
                  }

                  @Override
                  public boolean timeout(
                      ODistributedCoordinator coordinator, ORequestContext context) {
                    timedOut.countDown();
                    return true;
                  }
                });
          }

          @Override
          public void serialize(DataOutput output) throws IOException {}

          @Override
          public void deserialize(DataInput input) throws IOException {}

          @Override
          public int getRequestType() {
            return 0;
          }
        });

    // This is 2 seconds because timeout is hard coded with 1 sec now
    assertTrue(timedOut.await(2, TimeUnit.SECONDS));
    coordinator.close();
    assertEquals(0, coordinator.getContexts().size());
  }

  private static class MockNodeRequest implements ONodeRequest {

    @Override
    public ONodeResponse execute(
        ONodeIdentity nodeFrom,
        OLogId opId,
        ODistributedExecutor executor,
        ODatabaseDocumentInternal session) {
      return null;
    }

    @Override
    public void serialize(DataOutput output) throws IOException {}

    @Override
    public void deserialize(DataInput input) throws IOException {}

    @Override
    public int getRequestType() {
      return 0;
    }
  }

  private class MockNetword implements ODistributedNetwork {
    public ODistributedCoordinator coordinator;
    public CountDownLatch reply;

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
      reply.countDown();
    }

    @Override
    public void sendResponse(
        ONodeIdentity to, String database, OLogId opId, ONodeResponse response) {}

    @Override
    public void sendRequest(
        Collection<ONodeIdentity> to, String database, OLogId id, ONodeRequest request) {
      for (ONodeIdentity member : to) {
        coordinator.receive(
            member,
            id,
            new ONodeResponse() {
              @Override
              public void serialize(DataOutput output) throws IOException {}

              @Override
              public void deserialize(DataInput input) throws IOException {}

              @Override
              public int getResponseType() {
                return 0;
              }
            });
      }
    }

    @Override
    public void send(ONodeIdentity identity, OOperation fullConfiguration) {}

    @Override
    public void sendAll(Collection<ONodeIdentity> members, OOperation operation) {}

    @Override
    public void notifyLastDbOperation(
        ONodeIdentity leader, String database, OLogId leaderLastValid) {}

    @Override
    public void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid) {}
  }
}
