package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.*;
import com.orientechnologies.orient.distributed.impl.structural.*;

public class OCoordinateMessagesFactory {
  public static final int TRANSACTION_SUBMIT_REQUEST        = 1;
  public static final int TRANSACTION_SUBMIT_RESPONSE       = 1;
  public static final int TRANSACTION_FIRST_PHASE_REQUEST   = 1;
  public static final int TRANSACTION_FIRST_PHASE_RESPONSE  = 1;
  public static final int TRANSACTION_SECOND_PHASE_REQUEST  = 2;
  public static final int TRANSACTION_SECOND_PHASE_RESPONSE = 2;

  public static final int SEQUENCE_ACTION_COORDINATOR_SUBMIT   = 2;
  public static final int SEQUENCE_ACTION_COORDINATOR_RESPONSE = 2;
  public static final int SEQUENCE_ACTION_NODE_REQUEST         = 3;
  public static final int SEQUENCE_ACTION_NODE_RESPONSE        = 3;

  public static final int DDL_QUERY_SUBMIT_REQUEST  = 3;
  public static final int DDL_QUERY_SUBMIT_RESPONSE = 3;
  public static final int DDL_QUERY_NODE_REQUEST    = 4;
  public static final int DDL_QUERY_NODE_RESPONSE   = 4;

  //STRUCTURAL MESSAGES
  public static final int CREATE_DATABASE_SUBMIT_REQUEST    = 1;
  public static final int CREATE_DATABASE_SUBMIT_RESPONSE   = 1;
  public static final int CREATE_DATABASE_REQUEST           = 1;
  public static final int CREATE_DATABASE_RESPONSE          = 1;
  public static final int CREATE_DATABASE_FINALIZE_REQUEST  = 3;
  public static final int CREATE_DATABASE_FINALIZE_RESPONSE = 3;

  public static final int DROP_DATABASE_SUBMIT_REQUEST  = 2;
  public static final int DROP_DATABASE_SUBMIT_RESPONSE = 2;
  public static final int DROP_DATABASE_REQUEST         = 2;
  public static final int DROP_DATABASE_RESPONSE        = 2;

  public static final int CONFIGURATION_FETCH_SUBMIT_REQUEST  = 4;
  public static final int CONFIGURATION_FETCH_SUBMIT_RESPONSE = 4;

  public ONodeResponse createOperationResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_FIRST_PHASE_RESPONSE:
      return new OTransactionFirstPhaseResult();
    case TRANSACTION_SECOND_PHASE_RESPONSE:
      return new OTransactionSecondPhaseResponse();
    case SEQUENCE_ACTION_NODE_RESPONSE:
      return new OSequenceActionNodeResponse();
    case DDL_QUERY_NODE_RESPONSE:
      return new ODDLQueryOperationResponse();

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
    case DDL_QUERY_NODE_REQUEST:
      return new ODDLQueryOperationRequest();

    }
    return null;
  }

  public OSubmitRequest createSubmitRequest(int requestType) {
    switch (requestType) {
    case TRANSACTION_SUBMIT_REQUEST:
      return new OTransactionSubmit();
    case SEQUENCE_ACTION_COORDINATOR_SUBMIT:
      return new OSequenceActionCoordinatorSubmit();
    case DDL_QUERY_SUBMIT_REQUEST:
      return new ODDLQuerySubmitRequest();

    }
    return null;
  }

  public OSubmitResponse createSubmitResponse(int responseType) {
    switch (responseType) {
    case TRANSACTION_SUBMIT_RESPONSE:
      return new OTransactionResponse();
    case SEQUENCE_ACTION_COORDINATOR_RESPONSE:
      return new OSequenceActionCoordinatorResponse();
    case DDL_QUERY_SUBMIT_RESPONSE:
      return new ODDLQuerySubmitResponse();

    }
    return null;
  }

  public OStructuralNodeResponse createStructuralOperationResponse(int responseType) {
    switch (responseType) {
    case CREATE_DATABASE_RESPONSE:
      return new OCreateDatabaseOperationResponse();
    case CREATE_DATABASE_FINALIZE_RESPONSE:
      return new OCreateDatabaseFinalizeResponse();
    case DROP_DATABASE_RESPONSE:
      return new ODropDatabaseOperationResponse();
    }
    return null;
  }

  public OStructuralNodeRequest createStructuralOperationRequest(int requestType) {
    switch (requestType) {
    case CREATE_DATABASE_REQUEST:
      return new OCreateDatabaseOperationRequest();
    case CREATE_DATABASE_FINALIZE_REQUEST:
      return new OCreateDatabaseFinalizeRequest();
    case DROP_DATABASE_REQUEST:
      return new ODropDatabaseOperationRequest();
    }
    return null;
  }

  public OStructuralSubmitRequest createStructuralSubmitRequest(int requestType) {
    switch (requestType) {
    case CREATE_DATABASE_SUBMIT_REQUEST:
      return new OCreateDatabaseSubmitRequest();
    case DROP_DATABASE_SUBMIT_REQUEST:
      return new ODropDatabaseSubmitRequest();
    }
    return null;
  }

  public OStructuralSubmitResponse createStructuralSubmitResponse(int responseType) {
    switch (responseType) {
    case CREATE_DATABASE_SUBMIT_RESPONSE:
      return new OCreateDatabaseSubmitResponse();
    case DROP_DATABASE_SUBMIT_RESPONSE:
      return new ODropDatabaseSubmitResponse();
    }
    return null;
  }
}
