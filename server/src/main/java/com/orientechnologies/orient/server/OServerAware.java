package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;

/** Created by tglman on 14/08/17. */
public interface OServerAware {

  void init(OServer server);

  OClusterConfiguration getClusterConfiguration();

  void registerLifecycleListener(ODistributedLifecycleListener iListener);

  void unregisterLifecycleListener(ODistributedLifecycleListener iListener);
}
