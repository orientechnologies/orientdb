package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.orient.server.distributed.ODistributedLockManager;

import java.util.concurrent.TimeUnit;

public class OHazelcastLockManager implements ODistributedLockManager {
  public OHazelcastLockManager(HazelcastInstance hazelcast) {
    this.hazelcast = hazelcast;
  }

  private HazelcastInstance hazelcast;

  @Override
  public void acquireExclusiveLock(String resource, String nodeSource, long timeout) {
    try {
      hazelcast.getLock(resource).tryLock(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void releaseExclusiveLock(String resource, String nodeSource) {
    hazelcast.getLock(resource).unlock();
  }

  @Override
  public void handleUnreachableServer(String nodeLeftName) {

  }

  @Override
  public void shutdown() {

  }
}
