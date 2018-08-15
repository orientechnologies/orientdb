package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

public class FirstPhaseResponseHandlerTest {

  @Mock
  private ODistributedCoordinator coordinator;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFirstPhaseQuorumSuccess() {
    ODistributedMember member1 = new ODistributedMember("one", null);
    ODistributedMember member2 = new ODistributedMember("two", null);
    ODistributedMember member3 = new ODistributedMember("three", null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler();
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler
        .receive(coordinator, context, member1, new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null));
    handler
        .receive(coordinator, context, member2, new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null));
    handler
        .receive(coordinator, context, member3, new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null));

    Mockito.verify(coordinator, Mockito.atLeastOnce())
        .sendOperation(Mockito.any(OSubmitRequest.class), Mockito.eq(new OTransactionSecondPhaseOperation(true)),
            Mockito.any(OTransactionSecondPhaseHandler.class));
  }

  @Test
  public void testFirstPhaseQuorumCME() {
    ODistributedMember member1 = new ODistributedMember("one", null);
    ODistributedMember member2 = new ODistributedMember("two", null);
    ODistributedMember member3 = new ODistributedMember("three", null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler();
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(coordinator, context, member1,
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.CONCURRENT_MODIFICATION_EXCEPTION,
            new OTransactionFirstPhaseResult.ConcurrentModification(new ORecordId(10, 10), 0, 1)));
    handler
        .receive(coordinator, context, member2, new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null));
    handler.receive(coordinator, context, member3,
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.CONCURRENT_MODIFICATION_EXCEPTION,
            new OTransactionFirstPhaseResult.ConcurrentModification(new ORecordId(10, 10), 0, 1)));

    Mockito.verify(coordinator, Mockito.atLeastOnce())
        .sendOperation(Mockito.any(OSubmitRequest.class), Mockito.eq(new OTransactionSecondPhaseOperation(false)),
            Mockito.any(OTransactionSecondPhaseHandler.class));
  }

  @Test
  public void testFirstPhaseQuorumUnique() {
    ODistributedMember member1 = new ODistributedMember("one", null);
    ODistributedMember member2 = new ODistributedMember("two", null);
    ODistributedMember member3 = new ODistributedMember("three", null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler();
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(coordinator, context, member1,
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.UNIQUE_KEY_VIOLATION,
            new OTransactionFirstPhaseResult.UniqueKeyViolation("Key", new ORecordId(10, 10), new ORecordId(10, 11),
                "Class.property")));
    handler
        .receive(coordinator, context, member2, new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null));
    handler.receive(coordinator, context, member3,
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.UNIQUE_KEY_VIOLATION,
            new OTransactionFirstPhaseResult.UniqueKeyViolation("Key", new ORecordId(10, 10), new ORecordId(10, 11),
                "Class.property")));

    Mockito.verify(coordinator, Mockito.atLeastOnce())
        .sendOperation(Mockito.any(OSubmitRequest.class), Mockito.eq(new OTransactionSecondPhaseOperation(false)),
            Mockito.any(OTransactionSecondPhaseHandler.class));
  }

}
