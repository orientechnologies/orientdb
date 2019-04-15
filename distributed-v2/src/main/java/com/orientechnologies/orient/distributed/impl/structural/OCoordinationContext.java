package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;

public interface OCoordinationContext {

  OStructuralRequestContext sendOperation(OStructuralNodeRequest nodeRequest, OStructuralResponseHandler handler);

  void reply(OStructuralSubmitId id, OStructuralSubmitResponse response);

  OrientDBDistributed getOrientDB();
}
