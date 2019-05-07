package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;

public interface OOperation {
  void apply(OrientDBDistributed context);
}
