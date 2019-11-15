package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.distributed.OSyncRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQueryOperationResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.*;
import com.orientechnologies.orient.distributed.impl.structural.*;
import com.orientechnologies.orient.distributed.impl.structural.operations.*;
import com.orientechnologies.orient.distributed.impl.structural.raft.OCreateDatabase;
import com.orientechnologies.orient.distributed.impl.structural.raft.ODropDatabase;
import com.orientechnologies.orient.distributed.impl.structural.raft.ONodeJoin;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;

public class OCoordinateMessagesFactory {
  public static final int TRANSACTION_SUBMIT_REQUEST           = 1;
  public static final int TRANSACTION_SUBMIT_RESPONSE          = 2;
  public static final int TRANSACTION_FIRST_PHASE_REQUEST      = 3;
  public static final int TRANSACTION_FIRST_PHASE_RESPONSE     = 4;
  public static final int TRANSACTION_SECOND_PHASE_REQUEST     = 5;
  public static final int TRANSACTION_SECOND_PHASE_RESPONSE    = 6;
  public static final int SEQUENCE_ACTION_COORDINATOR_SUBMIT   = 7;
  public static final int SEQUENCE_ACTION_COORDINATOR_RESPONSE = 8;
  public static final int SEQUENCE_ACTION_NODE_REQUEST         = 9;
  public static final int SEQUENCE_ACTION_NODE_RESPONSE        = 10;
  public static final int DDL_QUERY_SUBMIT_REQUEST             = 11;
  public static final int DDL_QUERY_SUBMIT_RESPONSE            = 12;
  public static final int DDL_QUERY_NODE_REQUEST               = 13;
  public static final int DDL_QUERY_NODE_RESPONSE              = 14;
  public static final int CREATE_DATABASE_SUBMIT_REQUEST       = 15;
  public static final int CREATE_DATABASE_SUBMIT_RESPONSE      = 16;
  public static final int CREATE_DATABASE_REQUEST              = 17;
  public static final int DROP_DATABASE_REQUEST                = 18;
  public static final int CREATE_DATABASE_RESPONSE             = 19;
  public static final int NODE_JOIN_REQUEST                    = 20;
  public static final int CREATE_DATABASE_FINALIZE_REQUEST     = 21;
  public static final int CREATE_DATABASE_FINALIZE_RESPONSE    = 22;
  public static final int DROP_DATABASE_SUBMIT_REQUEST         = 23;
  public static final int DROP_DATABASE_SUBMIT_RESPONSE        = 24;
  public static final int DROP_DATABASE_RESPONSE               = 25;
  public static final int CONFIGURATION_FETCH_SUBMIT_REQUEST   = 26;
  public static final int CONFIGURATION_FETCH_SUBMIT_RESPONSE  = 27;
  public static final int SYNC_SUBMIT_REQUEST                  = 28;

  public static ONodeResponse createOperationResponse(int responseType) {
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

  public static ONodeRequest createOperationRequest(int requestType) {
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

  public static OSubmitRequest createSubmitRequest(int requestType) {
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

  public static OSubmitResponse createSubmitResponse(int responseType) {
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

  public static OStructuralSubmitRequest createStructuralSubmitRequest(int requestType) {
    switch (requestType) {
    case CREATE_DATABASE_SUBMIT_REQUEST:
      return new OCreateDatabaseSubmitRequest();
    case DROP_DATABASE_SUBMIT_REQUEST:
      return new ODropDatabaseSubmitRequest();
    case SYNC_SUBMIT_REQUEST:
      return new OSyncRequest();
    }
    return null;
  }

  public static OStructuralSubmitResponse createStructuralSubmitResponse(int responseType) {
    switch (responseType) {
    case CREATE_DATABASE_SUBMIT_RESPONSE:
      return new OCreateDatabaseSubmitResponse();
    case DROP_DATABASE_SUBMIT_RESPONSE:
      return new ODropDatabaseSubmitResponse();
    }
    return null;
  }

  public static ORaftOperation createRaftOperation(int requestType) {
    switch (requestType) {
    case CREATE_DATABASE_REQUEST:
      return new OCreateDatabase();
    case NODE_JOIN_REQUEST:
      return new ONodeJoin();
    case DROP_DATABASE_REQUEST:
      return new ODropDatabase();
    }

    return null;

  }
}
