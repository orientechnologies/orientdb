package com.orientechnologies.orient.server.hazelcast.sharding.distributed;

import java.util.Set;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 9/10/12
 */
public interface ODHTConfiguration {
  Set<String> getDistributedStorageNames();

  Set<String> getUndistributableClusters();
}
