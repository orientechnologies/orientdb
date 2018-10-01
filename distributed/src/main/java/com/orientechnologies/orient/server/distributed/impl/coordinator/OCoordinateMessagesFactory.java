package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.*;

public class OCoordinateMessagesFactory {
  public static final int TRANSACTION_SUBMIT_REQUEST        = 1;
  public static final int TRANSACTION_SUBMIT_RESPONSE       = 1;
  public static final int TRANSACTION_FIRST_PHASE_REQUEST   = 1;
  public static final int TRANSACTION_FIRST_PHASE_RESPONSE  = 1;
  public static final int TRANSACTION_SECOND_PHASE_REQUEST  = 2;
  public static final int TRANSACTION_SECOND_PHASE_RESPONSE = 2;  
  public static final int SEQUENCE_ACTION_COORDINATOR_SUBMIT   = 3;
  public static final int SEQUENCE_ACTION_COORDINATOR_RESPONSE  = 3;
  public static final int SEQUENCE_ACTION_NODE_REQUEST           = 4;
  public static final int SEQUENCE_ACTION_NODE_RESPONSE          = 4;

  public ONodeResponse createOperationResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_FIRST_PHASE_RESPONSE:
      return new OTransactionFirstPhaseResult();
    case TRANSACTION_SECOND_PHASE_RESPONSE:
      return new OTransactionSecondPhaseResponse();
    case SEQUENCE_ACTION_NODE_RESPONSE:
      return new OSequenceActionNodeResponse();
    }
    return null;
  }

  public ONodeRequest createOperationRequest(int requestType) {
    switch (requestType) {
    case TRANSACTION_FIRST_PHASE_REQUEST:
      return new OTransactionFirstPhaseOperation();
    case TRANSACTION_SECOND_PHASE_REQUEST:
      return new OTransactionSecondPhaseOperation();
    case SEQUENCE_ACTION_NODE_REQUEST:
      return new OSequenceActionNodeRequest();
    }
    return null;
  }

  public OSubmitRequest createSubmitRequest(int requestType) {
    switch (requestType) {
    case TRANSACTION_SUBMIT_REQUEST:
      return new OTransactionSubmit();
    case SEQUENCE_ACTION_COORDINATOR_SUBMIT:
      return new OSequenceActionCoordinatorSubmit();
    }
    return null;
  }

  public OSubmitResponse createSubmitResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_SUBMIT_RESPONSE:
      return new OTransactionResponse();
    case SEQUENCE_ACTION_COORDINATOR_RESPONSE:
      return new OSequenceActionCoordinatorResponse();
    }
    return null;
  }

}
