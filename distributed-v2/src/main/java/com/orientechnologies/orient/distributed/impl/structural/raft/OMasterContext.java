package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.structural.*;

public interface OMasterContext {
  interface OpFinished {
    void finished();
  }

  void propagateAndApply(ORaftOperation operation, OpFinished finished);

  OrientDBDistributed getOrientDB();

}
