package com.orientechnologies.orient.core.index.hashindex.local.cache;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@State(Scope.Benchmark)
public class Cache {
  public LRUList cache = new SynchronizedLRUList();

  @Param({ "concurrent", "synchronized" })
  private String cacheType;

  @Setup(Level.Iteration)
  public void up() {
    if ("concurrent".equals(cacheType)) {
      cache = new ConcurrentLRUList();
    } else {
      cache = new SynchronizedLRUList();
    }
  }

  public LRUList get() {
    return cache;
  }
}
