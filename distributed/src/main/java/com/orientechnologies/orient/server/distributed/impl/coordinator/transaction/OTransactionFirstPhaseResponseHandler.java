package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.ConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.UniqueKeyViolation;

import java.util.*;

public class OTransactionFirstPhaseResponseHandler implements OResponseHandler {

  private final OSessionOperationId                   operationId;
  private final OTransactionSubmit                    request;
  private final ODistributedMember                    requester;
  private       int                                   responseCount   = 0;
  private final Set<ODistributedMember>               success         = new HashSet<>();
  private final Map<ORID, List<ODistributedMember>>   cme             = new HashMap<>();
  private final Map<String, List<ODistributedMember>> unique          = new HashMap<>();
  private final List<ODistributedMember>              exceptions      = new ArrayList<>();
  private       boolean                               secondPhaseSent = false;
  private       boolean                               replySent       = false;
  private final List<OLockGuard>                      guards;

  public OTransactionFirstPhaseResponseHandler(OSessionOperationId operationId, OTransactionSubmit request,
      ODistributedMember requester, List<OLockGuard> guards) {
    this.operationId = operationId;
    this.request = request;
    this.requester = requester;
    this.guards = guards;
  }

  @Override
  public boolean receive(ODistributedCoordinator coordinator, ORequestContext context, ODistributedMember member,
      ONodeResponse response) {
    responseCount++;
    OTransactionFirstPhaseResult result = (OTransactionFirstPhaseResult) response;
    switch (result.getType()) {
    case SUCCESS:
      success.add(member);
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
    case EXCEPTION:
      exceptions.add(member);
      break;
    }
    int quorum = context.getQuorum();
    if (responseCount >= quorum && !secondPhaseSent) {
      if (success.size() >= quorum) {
        sendSecondPhaseSuccess(coordinator);
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
    }

    return responseCount == context.getInvolvedMembers().size();
  }

  private void sendSecondPhaseError(ODistributedCoordinator coordinator) {
    OTransactionSecondPhaseResponseHandler responseHandler = new OTransactionSecondPhaseResponseHandler(true, request, requester,
        null);
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(operationId, false), responseHandler);
    if (guards != null) {
      for (OLockGuard guard : guards) {
        guard.release();
      }
    }
    if (!replySent) {
      coordinator.reply(requester, new OTransactionResponse());
      replySent = true;
    }
    secondPhaseSent = true;
  }

  private void sendSecondPhaseSuccess(ODistributedCoordinator coordinator) {
    OTransactionSecondPhaseResponseHandler responseHandler = new OTransactionSecondPhaseResponseHandler(false, request, requester,
        guards);
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(operationId, true), responseHandler);
    secondPhaseSent = true;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
