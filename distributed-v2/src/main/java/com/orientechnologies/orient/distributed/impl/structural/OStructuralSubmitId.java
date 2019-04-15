package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OStructuralSubmitId {

  private OStructuralDistributedMember member;
  private OSessionOperationId          operationId;

  public OStructuralSubmitId(OStructuralDistributedMember member, OSessionOperationId operationId) {
    this.member = member;
    this.operationId = operationId;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }

  public OStructuralDistributedMember getMember() {
    return member;
  }
}
