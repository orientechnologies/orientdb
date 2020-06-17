package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.Type;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class FirstPhaseResponseHandlerTest {

  @Mock private ODistributedCoordinator coordinator;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFirstPhaseQuorumSuccess() {
    OSessionOperationId operationId = new OSessionOperationId();
    ONodeIdentity member1 = new ONodeIdentity("one", "one");
    ONodeIdentity member2 = new ONodeIdentity("two", "two");
    ONodeIdentity member3 = new ONodeIdentity("three", "three");
    List<ONodeIdentity> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler =
        new OTransactionFirstPhaseResponseHandler(
            operationId, null, member1, new ArrayList<>(), new ArrayList<>(), null);
    OLogId id = new OLogId(1, 0, 0);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(
        coordinator, context, member1, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(
        coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(
        coordinator, context, member3, new OTransactionFirstPhaseResult(Type.SUCCESS, null));

    Mockito.verify(coordinator, times(1))
        .sendOperation(
            any(OSubmitRequest.class),
            eq(
                new OTransactionSecondPhaseOperation(
                    operationId, new ArrayList<>(), new ArrayList<>(), true)),
            any(OTransactionSecondPhaseResponseHandler.class));
    Mockito.verify(coordinator, times(0))
        .reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));
  }

  @Test
  public void testFirstPhaseQuorumCME() {
    OSessionOperationId operationId = new OSessionOperationId();
    ONodeIdentity member1 = new ONodeIdentity("one", "one");
    ONodeIdentity member2 = new ONodeIdentity("two", "two");
    ONodeIdentity member3 = new ONodeIdentity("three", "three");
    List<ONodeIdentity> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler =
        new OTransactionFirstPhaseResponseHandler(
            operationId, null, member1, new ArrayList<>(), new ArrayList<>(), null);
    OLogId id = new OLogId(1, 0, 0);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(
        coordinator,
        context,
        member1,
        new OTransactionFirstPhaseResult(
            Type.CONCURRENT_MODIFICATION_EXCEPTION,
            new OConcurrentModificationResult(new ORecordId(10, 10), 0, 1)));
    handler.receive(
        coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(
        coordinator,
        context,
        member3,
        new OTransactionFirstPhaseResult(
            Type.CONCURRENT_MODIFICATION_EXCEPTION,
            new OConcurrentModificationResult(new ORecordId(10, 10), 0, 1)));

    Mockito.verify(coordinator, times(1))
        .sendOperation(
            any(OSubmitRequest.class),
            eq(
                new OTransactionSecondPhaseOperation(
                    operationId, new ArrayList<>(), new ArrayList<>(), false)),
            any(OTransactionSecondPhaseResponseHandler.class));

    Mockito.verify(coordinator, times(1))
        .reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));
  }

  @Test
  public void testFirstPhaseQuorumUnique() {
    OSessionOperationId operationId = new OSessionOperationId();
    ONodeIdentity member1 = new ONodeIdentity("one", "one");
    ONodeIdentity member2 = new ONodeIdentity("two", "two");
    ONodeIdentity member3 = new ONodeIdentity("three", "three");
    List<ONodeIdentity> members = new ArrayList<>();
    members.add(member1);
    members.add(member2);
    members.add(member3);
    OTransactionFirstPhaseResponseHandler handler =
        new OTransactionFirstPhaseResponseHandler(
            operationId, null, member1, new ArrayList<>(), new ArrayList<>(), null);
    OLogId id = new OLogId(1, 0, 0);
    ORequestContext context = new ORequestContext(null, null, null, members, handler, id);

    handler.receive(
        coordinator,
        context,
        member1,
        new OTransactionFirstPhaseResult(
            Type.UNIQUE_KEY_VIOLATION,
            new OUniqueKeyViolationResult(
                "Key", new ORecordId(10, 10), new ORecordId(10, 11), "Class.property")));
    handler.receive(
        coordinator, context, member2, new OTransactionFirstPhaseResult(Type.SUCCESS, null));
    handler.receive(
        coordinator,
        context,
        member3,
        new OTransactionFirstPhaseResult(
            Type.UNIQUE_KEY_VIOLATION,
            new OUniqueKeyViolationResult(
                "Key", new ORecordId(10, 10), new ORecordId(10, 11), "Class.property")));

    Mockito.verify(coordinator, times(1))
        .sendOperation(
            any(OSubmitRequest.class),
            eq(
                new OTransactionSecondPhaseOperation(
                    operationId, new ArrayList<>(), new ArrayList<>(), false)),
            any(OTransactionSecondPhaseResponseHandler.class));

    Mockito.verify(coordinator, times(1))
        .reply(same(member1), any(OSessionOperationId.class), any(OTransactionResponse.class));
  }
}
