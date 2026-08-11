package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.id.ODatabaseId;
import com.orientechnologies.orient.core.id.ONodeId;

public interface ODatabaseStateChangeListener {
  void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state);
}
