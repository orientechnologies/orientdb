package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OTransactionFirstPhaseResponseHandler implements OResponseHandler {

  private int                                      responseCount   = 0;
  private List<ODistributedMember>                 success         = new ArrayList<>();
  private Map<ORID, List<ODistributedMember>>      cme             = new HashMap<>();
  private Map<String, List<ODistributedMember>>    unique          = new HashMap<>();
  private Map<ORecordId, List<ODistributedMember>> pessimisticLock = new HashMap<>();
  private List<ODistributedMember>                 exceptions      = new ArrayList<>();
  private boolean                                  secondPhaseSent = false;
  private boolean                                  replySent       = false;

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
      OTransactionFirstPhaseResult.ConcurrentModification concurrentModification = (OTransactionFirstPhaseResult.ConcurrentModification) result
          .getResultMetadata();
      List<ODistributedMember> members = cme.get(concurrentModification.getRecordId());
      if (members == null) {
        members = new ArrayList<>();
        cme.put(concurrentModification.getRecordId(), members);
      }
      members.add(member);
    }
    break;
    case UNIQUE_KEY_VIOLATION: {
      OTransactionFirstPhaseResult.UniqueKeyViolation uniqueKeyViolation = (OTransactionFirstPhaseResult.UniqueKeyViolation) result
          .getResultMetadata();
      List<ODistributedMember> members = unique.get(uniqueKeyViolation.getKeyStringified());
      if (members == null) {
        members = new ArrayList<>();
        unique.put(uniqueKeyViolation.getKeyStringified(), members);
      }
      members.add(member);
    }
    break;
    case PESSIMISTIC_LOCK_TIMEOUT: {
      OTransactionFirstPhaseResult.PessimisticLockTimeout pessimisticLockTimeout = (OTransactionFirstPhaseResult.PessimisticLockTimeout) result
          .getResultMetadata();
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

      for (Map.Entry<ORecordId, List<ODistributedMember>> entry : pessimisticLock.entrySet()) {
        if (entry.getValue().size() >= quorum) {
          sendSecondPhaseError(coordinator);
        }
      }
    }

    return responseCount == context.getInvolvedMembers().size();
  }

  private void sendSecondPhaseError(ODistributedCoordinator coordinator) {
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(false), new OTransactionSecondPhaseHandler(true));
    secondPhaseSent = true;
  }

  private void sendSecondPhaseSuccess(ODistributedCoordinator coordinator) {
    coordinator.sendOperation(null, new OTransactionSecondPhaseOperation(true), new OTransactionSecondPhaseHandler(false));
    secondPhaseSent = true;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
