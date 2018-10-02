package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.Type;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;

public class FirstPhaseResponseHandlerTest {

  @Mock
  private ODatabaseCoordinator coordinator;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFirstPhaseQuorumSuccess() {
    OSessionOperationId operationId = new OSessionOperationId();
    ODistributedMember member1 = new ODistributedMember("one", null, null);
    ODistributedMember member2 = new ODistributedMember("two", null, null);
    ODistributedMember member3 = new ODistributedMember("three", null, null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler(operationId, null, member1, null);
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(coordinator, context, member1, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(coordinator, context, member3, new OTransactionFirstPhaseResult(Type.SUCCESS, null));

    Mockito.verify(coordinator, times(1))
        .sendOperation(any(OSubmitRequest.class), eq(new OTransactionSecondPhaseOperation(operationId, true)),
            any(OTransactionSecondPhaseResponseHandler.class));
    Mockito.verify(coordinator, times(0)).reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));
  }

  @Test
  public void testFirstPhaseQuorumCME() {
    OSessionOperationId operationId = new OSessionOperationId();
    ODistributedMember member1 = new ODistributedMember("one", null, null);
    ODistributedMember member2 = new ODistributedMember("two", null, null);
    ODistributedMember member3 = new ODistributedMember("three", null, null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler(operationId, null, member1, null);
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(coordinator, context, member1, new OTransactionFirstPhaseResult(Type.CONCURRENT_MODIFICATION_EXCEPTION,
        new OConcurrentModificationResult(new ORecordId(10, 10), 0, 1)));
    handler.receive(coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(coordinator, context, member3, new OTransactionFirstPhaseResult(Type.CONCURRENT_MODIFICATION_EXCEPTION,
        new OConcurrentModificationResult(new ORecordId(10, 10), 0, 1)));

    Mockito.verify(coordinator, times(1))
        .sendOperation(any(OSubmitRequest.class), eq(new OTransactionSecondPhaseOperation(operationId, false)),
            any(OTransactionSecondPhaseResponseHandler.class));

    Mockito.verify(coordinator, times(1)).reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));
  }

  @Test
  public void testFirstPhaseQuorumUnique() {
    OSessionOperationId operationId = new OSessionOperationId();
    ODistributedMember member1 = new ODistributedMember("one", null, null);
    ODistributedMember member2 = new ODistributedMember("two", null, null);
    ODistributedMember member3 = new ODistributedMember("three", null, null);
    List<ODistributedMember> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler = new OTransactionFirstPhaseResponseHandler(operationId, null, member1, null);
    OLogId id = new OLogId(1);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(coordinator, context, member1, new OTransactionFirstPhaseResult(Type.UNIQUE_KEY_VIOLATION,
        new OUniqueKeyViolationResult("Key", new ORecordId(10, 10), new ORecordId(10, 11), "Class.property")));
    handler.receive(coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(coordinator, context, member3, new OTransactionFirstPhaseResult(Type.UNIQUE_KEY_VIOLATION,
        new OUniqueKeyViolationResult("Key", new ORecordId(10, 10), new ORecordId(10, 11), "Class.property")));

    Mockito.verify(coordinator, times(1))
        .sendOperation(any(OSubmitRequest.class), eq(new OTransactionSecondPhaseOperation(operationId, false)),
            any(OTransactionSecondPhaseResponseHandler.class));

    Mockito.verify(coordinator, times(1)).reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));

  }

}
