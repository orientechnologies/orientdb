package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/** Created by tglman on 14/08/17. */
public interface OServerAware {

  void init(OServer server);

  ODistributedServerManager getDistributedManager();
}
