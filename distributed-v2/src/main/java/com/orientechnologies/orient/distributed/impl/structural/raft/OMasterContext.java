package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.structural.*;

public interface OMasterContext {

  void propagateAndApply(ORaftOperation operation);

  OrientDBDistributed getOrientDB();

}
