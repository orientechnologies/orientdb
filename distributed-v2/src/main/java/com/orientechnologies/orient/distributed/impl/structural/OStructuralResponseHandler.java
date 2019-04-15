package com.orientechnologies.orient.distributed.impl.structural;

public interface OStructuralResponseHandler {
  boolean receive(OCoordinationContext coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response);

  boolean timeout(OCoordinationContext coordinator, OStructuralRequestContext context);
}
