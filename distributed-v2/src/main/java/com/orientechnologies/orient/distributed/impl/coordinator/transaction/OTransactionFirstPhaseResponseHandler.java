package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OResponseHandler;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.OLockGuard;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OTransactionFirstPhaseResponseHandler implements OResponseHandler {

  private final OSessionOperationId operationId;
  private final OTransactionSubmit request;
  private final ONodeIdentity requester;
  private int responseCount = 0;
  private final Set<ONodeIdentity> success = new HashSet<>();
  private final Map<ORID, List<ONodeIdentity>> cme = new HashMap<>();
  private final Map<String, List<ONodeIdentity>> unique = new HashMap<>();
  private final List<ONodeIdentity> exceptions = new ArrayList<>();
  private boolean secondPhaseSent = false;
  private boolean replySent = false;
  private final List<OLockGuard> guards;
  private List<ORecordOperationRequest> operations;
  private List<OIndexOperationRequest> indexes;

  public OTransactionFirstPhaseResponseHandler(
      OSessionOperationId operationId,
      OTransactionSubmit request,
      ONodeIdentity requester,
      List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes,
      List<OLockGuard> guards) {
    this.operationId = operationId;
    this.request = request;
    this.requester = requester;
    this.guards = guards;
    this.operations = operations;
    this.indexes = indexes;
  }

  @Override
  public boolean receive(
      ODistributedCoordinator coordinator,
      ORequestContext context,
      ONodeIdentity member,
      ONodeResponse response) {
    responseCount++;
    OTransactionFirstPhaseResult result = (OTransactionFirstPhaseResult) response;
    switch (result.getType()) {
      case SUCCESS:
        success.add(member);
        break;
      case CONCURRENT_MODIFICATION_EXCEPTION:
        {
          OConcurrentModificationResult concurrentModification =
              (OConcurrentModificationResult) result.getResultMetadata();
          List<ONodeIdentity> members = cme.get(concurrentModification.getRecordId());
          if (members == null) {
            members = new ArrayList<>();
            cme.put(concurrentModification.getRecordId(), members);
          }
          members.add(member);
        }
        break;
      case UNIQUE_KEY_VIOLATION:
        {
          OUniqueKeyViolationResult uniqueKeyViolation =
              (OUniqueKeyViolationResult) result.getResultMetadata();
          List<ONodeIdentity> members = unique.get(uniqueKeyViolation.getKeyStringified());
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

      for (Map.Entry<ORID, List<ONodeIdentity>> entry : cme.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
          break;
        }
      }

      for (Map.Entry<String, List<ONodeIdentity>> entry : unique.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
          break;
        }
      }

      if (responseCount == context.getInvolvedMembers().size()) {
        sendSecondPhaseError(coordinator);
      }
    }
    return responseCount == context.getInvolvedMembers().size();
  }

  private void sendSecondPhaseError(ODistributedCoordinator coordinator) {
    if (secondPhaseSent) return;
    OTransactionSecondPhaseResponseHandler responseHandler =
        new OTransactionSecondPhaseResponseHandler(false, request, requester, null, operationId);
    coordinator.sendOperation(
        null,
        new OTransactionSecondPhaseOperation(
            operationId, new ArrayList<>(), new ArrayList<>(), false),
        responseHandler);
    if (guards != null) {
      coordinator.getLockManager().unlock(guards);
    }
    if (!replySent) {
      coordinator.reply(
          requester,
          operationId,
          new OTransactionResponse(false, new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));
      replySent = true;
    }
    secondPhaseSent = true;
  }

  private void sendSecondPhaseSuccess(ODistributedCoordinator coordinator) {
    if (secondPhaseSent) return;
    OTransactionSecondPhaseResponseHandler responseHandler =
        new OTransactionSecondPhaseResponseHandler(true, request, requester, guards, operationId);
    coordinator.sendOperation(
        null,
        new OTransactionSecondPhaseOperation(operationId, operations, indexes, true),
        responseHandler);
    secondPhaseSent = true;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
