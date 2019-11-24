package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.raft.OFullConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;

import java.util.Collection;

public interface ODistributedNetwork {

  /**
   * Structural forward
   */
  void submit(ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitRequest request);

  /**
   * Structural forward reply
   */
  void reply(ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitResponse response);


  /**
   * Structural Operation
   */
  void propagate(Collection<ONodeIdentity> to, OLogId id, ORaftOperation operation);

  /**
   * Structural Operation ack
   */
  void ack(ONodeIdentity to, OLogId logId);

  /**
   * Structural Operation confirmation
   */
  void confirm(Collection<ONodeIdentity> to, OLogId id);



  /**
   * Data operation forward
   */
  void submit(ONodeIdentity coordinator, String database, OSessionOperationId operationId, OSubmitRequest request);

  /**
   * Data operation reply
   */
  void replay(ONodeIdentity to, String database, OSessionOperationId operationId, OSubmitResponse response);


  /**
   * Data Request
   */
  void sendResponse(ONodeIdentity to, String database, OLogId opId, ONodeResponse response);

  /**
   * Data response
   */
  void sendRequest(Collection<ONodeIdentity> to, String database, OLogId id, ONodeRequest request);



  /**
   * Structural Full sync
   */
  void send(ONodeIdentity identity, OFullConfiguration fullConfiguration);

}
