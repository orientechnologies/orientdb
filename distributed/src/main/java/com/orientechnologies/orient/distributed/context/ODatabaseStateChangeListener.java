package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;

public interface ODatabaseStateChangeListener {
  void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state);
}
