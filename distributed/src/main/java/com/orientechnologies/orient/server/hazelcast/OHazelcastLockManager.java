package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.orient.server.distributed.ODistributedLockManager;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class OHazelcastLockManager implements ODistributedLockManager {
  public OHazelcastLockManager(HazelcastInstance hazelcast) {
    this.hazelcast = hazelcast;
  }

  private HazelcastInstance hazelcast;
  private Set<String>       lockedResurces = new HashSet<>();

  @Override
  public void acquireExclusiveLock(String resource, String nodeSource, long timeout) {
    if (timeout != 0) {
      try {
        hazelcast.getLock(resource).tryLock(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      hazelcast.getLock(resource).lock();
    }
    synchronized (this) {
      lockedResurces.add(resource);
    }
  }

  @Override
  public void releaseExclusiveLock(String resource, String nodeSource) {
    hazelcast.getLock(resource).unlock();
    synchronized (this) {
      lockedResurces.remove(resource);
    }
  }

  @Override
  public void handleUnreachableServer(String nodeLeftName) {

  }

  @Override
  public void shutdown() {
    synchronized (this) {
      HashSet<String> res = new HashSet<>(lockedResurces);
      for (String resource : res) {
        releaseExclusiveLock(resource, null);
      }
    }
  }
}
