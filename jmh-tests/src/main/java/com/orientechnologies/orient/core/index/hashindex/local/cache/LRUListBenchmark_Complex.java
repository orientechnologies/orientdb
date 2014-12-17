/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Benchmark for {@link ConcurrentLRUList}.
 *
 * To run this benchmark run {@link org.openjdk.jmh.Main}.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LRUListBenchmark_Complex {
  private Random r = new Random();

  @State(Scope.Group)
  public static class PreparedCache {
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

      for (int i = 0; i < 1000; i++) {
        cache.putToMRU(new OCacheEntry(0, i, null, false));
      }
    }

    public LRUList get() {
      return cache;
    }
  }

  @State(Scope.Thread)
  public static class KeyGenerator {
    private int  index = 0;
    private long fileId;

    public int next() {
      return index++;
    }

    @Setup
    public void setup(ThreadParams threadInfo) {
      fileId = threadInfo.getThreadIndex();
    }
  }

  @Benchmark
  @Group("thread1")
  @GroupThreads(1)
  public void testAdd_1thread(Cache cache, KeyGenerator key) {
    cache.get().putToMRU(new OCacheEntry(key.fileId, key.next(), null, false));
  }

  @Benchmark
  @Group("thread1")
  @GroupThreads(1)
  public void testRemoveLRU_1thread(Cache cache) {
    cache.get().removeLRU();
  }

  @Benchmark
  @Group("thread1")
  @GroupThreads(1)
  public void testGet_1thread(PreparedCache cache) {
    cache.get().get(0, r.nextInt(1000));
  }

  @Benchmark
  @Group("thread2")
  @GroupThreads(2)
  public void testGet_2thread(PreparedCache cache) {
    cache.get().get(0, r.nextInt(1000));
  }

  @Benchmark
  @Group("thread2")
  @GroupThreads(2)
  public void testAdd_2thread(Cache cache, KeyGenerator key) {
    cache.get().putToMRU(new OCacheEntry(key.fileId, key.next(), null, false));
  }

  @Benchmark
  @Group("thread2")
  @GroupThreads(2)
  public void testRemoveLRU_2thread(Cache cache) {
    cache.get().removeLRU();
  }

  @Benchmark
  @Group("thread4")
  @GroupThreads(4)
  public void testGet_4thread(PreparedCache cache) {
    cache.get().get(0, r.nextInt(1000));
  }

  @Benchmark
  @Group("thread4")
  @GroupThreads(4)
  public void testAdd_4thread(Cache cache, KeyGenerator key) {
    cache.get().putToMRU(new OCacheEntry(key.fileId, key.next(), null, false));
  }

  @Benchmark
  @Group("thread4")
  @GroupThreads(4)
  public void testRemoveLRU_4thread(Cache cache) {
    cache.get().removeLRU();
  }
}
