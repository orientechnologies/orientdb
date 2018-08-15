package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.ConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.PessimisticLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.Success;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.UniqueKeyViolation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OTransactionFirstPhaseResponseHandler implements OResponseHandler {

  private OSessionOperationId                      operationId;
  private OTransactionSubmit                       request;
  private ODistributedMember                       requester;
  private int                                      responseCount   = 0;
  private Map<ODistributedMember, Success>         success         = new HashMap<>();
  private Map<ORID, List<ODistributedMember>>      cme             = new HashMap<>();
  private Map<String, List<ODistributedMember>>    unique          = new HashMap<>();
  private Map<ORecordId, List<ODistributedMember>> pessimisticLock = new HashMap<>();
  private List<ODistributedMember>                 exceptions      = new ArrayList<>();
  private boolean                                  secondPhaseSent = false;
  private boolean                                  replySent       = false;

  public OTransactionFirstPhaseResponseHandler(OSessionOperationId operationId, OTransactionSubmit request,
      ODistributedMember requester) {
    this.operationId = operationId;
    this.request = request;
    this.requester = requester;
  }

  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    responseCount++;
    OTransactionFirstPhaseResult result = (OTransactionFirstPhaseResult) response;
    switch (result.getType()) {
    case SUCCESS:
      success.put(member, (Success) result.getResultMetadata());
      break;
    case CONCURRENT_MODIFICATION_EXCEPTION: {
      ConcurrentModification concurrentModification = (ConcurrentModification) result.getResultMetadata();
      List<ODistributedMember> members = cme.get(concurrentModification.getRecordId());
      if (members == null) {
        members = new ArrayList<>();
        cme.put(concurrentModification.getRecordId(), members);
      }
      members.add(member);
    }
    break;
    case UNIQUE_KEY_VIOLATION: {
      UniqueKeyViolation uniqueKeyViolation = (UniqueKeyViolation) result.getResultMetadata();
      List<ODistributedMember> members = unique.get(uniqueKeyViolation.getKeyStringified());
      if (members == null) {
        members = new ArrayList<>();
        unique.put(uniqueKeyViolation.getKeyStringified(), members);
      }
      members.add(member);
    }
    break;
    case PESSIMISTIC_LOCK_TIMEOUT: {
      PessimisticLockTimeout pessimisticLockTimeout = (PessimisticLockTimeout) result.getResultMetadata();
      List<ODistributedMember> members = pessimisticLock.get(pessimisticLockTimeout.getRecordId());
      if (members == null) {
        members = new ArrayList<>();
        pessimisticLock.put(pessimisticLockTimeout.getRecordId(), members);
      }
      members.add(member);
    }
    break;
    case EXCEPTION:
      exceptions.add(member);
      break;
    }
    int quorum = context.getQuorum();
    if (responseCount >= quorum && !secondPhaseSent) {
      if (success.size() >= quorum) {
        Success ids = success.values().iterator().next();
        sendSecondPhaseSuccess(coordinator, ids.getAllocatedIds());
      }

      for (Map.Entry<ORID, List<ODistributedMember>> entry : cme.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
        }
      }

      for (Map.Entry<String, List<ODistributedMember>> entry : unique.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
        }
      }

      for (Map.Entry<ORecordId, List<ODistributedMember>> entry : pessimisticLock.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
        }
      }
    }

    return responseCount == context.getInvolvedMembers().size();
  }

  private void sendSecondPhaseError(ODistributedCoordinator coordinator) {
    OTransactionSecondPhaseResponseHandler responseHandler = new OTransactionSecondPhaseResponseHandler(true, request, requester);
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(operationId, false, new ArrayList<>()), responseHandler);
    coordinator.reply(requester, new OTransactionResponse());
    secondPhaseSent = true;
  }

  private void sendSecondPhaseSuccess(ODistributedCoordinator coordinator, List<ORecordId> allocatedIds) {
    OTransactionSecondPhaseResponseHandler responseHandler = new OTransactionSecondPhaseResponseHandler(false, request, requester);
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(operationId, true, allocatedIds), responseHandler);
    secondPhaseSent = true;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
