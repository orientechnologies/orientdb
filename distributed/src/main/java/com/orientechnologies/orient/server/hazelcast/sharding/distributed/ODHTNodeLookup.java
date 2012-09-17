package com.orientechnologies.orient.server.hazelcast.sharding.distributed;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNodeLookup {
  public ODHTNode findById(long id);
}
