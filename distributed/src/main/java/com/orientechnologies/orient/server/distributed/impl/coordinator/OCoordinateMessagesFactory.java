package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.*;

public class OCoordinateMessagesFactory {
  public static final int TRANSACTION_SUBMIT_REQUEST        = 1;
  public static final int TRANSACTION_SUBMIT_RESPONSE       = 1;
  public static final int TRANSACTION_FIRST_PHASE_REQUEST   = 1;
  public static final int TRANSACTION_FIRST_PHASE_RESPONSE  = 1;
  public static final int TRANSACTION_SECOND_PHASE_REQUEST  = 2;
  public static final int TRANSACTION_SECOND_PHASE_RESPONSE = 2;

  public ONodeResponse createOperationResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_FIRST_PHASE_RESPONSE:
      return new OTransactionFirstPhaseResult();
    case TRANSACTION_SECOND_PHASE_RESPONSE:
      return new OTransactionSecondPhaseResponse();
    }
    return null;
  }

  public ONodeRequest createOperationRequest(int requestType) {
    switch (requestType) {
    case TRANSACTION_FIRST_PHASE_REQUEST:
      return new OTransactionFirstPhaseOperation();
    case TRANSACTION_SECOND_PHASE_REQUEST:
      return new OTransactionSecondPhaseOperation();

    }
    return null;
  }

  public OSubmitRequest createSubmitRequest(int requestType) {
    switch (requestType) {
    case TRANSACTION_SUBMIT_REQUEST:
      return new OTransactionSubmit();
    }
    return null;
  }

  public OSubmitResponse createSubmitResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_SUBMIT_RESPONSE:
      return new OTransactionResponse();
    }
    return null;
  }

}
