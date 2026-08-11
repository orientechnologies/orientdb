package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.id.ONodeId;

public interface ONetworkDiscover {

  void connected(ONodeId node, String host);
}
