/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.hazelcast.sharding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.ODHTNode;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * Abstract distributed executor. Contains methods to distribute command across all nodes
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:26
 */
public abstract class OAbstractDistributedQueryExecutor extends OQueryExecutor {

  protected final ExecutorService distributedQueryExecutors;
  protected final ServerInstance  serverInstance;

  protected final AtomicInteger   failedNodes = new AtomicInteger(0);

  protected OAbstractDistributedQueryExecutor(OCommandRequestText iCommand, OStorageEmbedded wrapped, ServerInstance serverInstance) {
    super(iCommand, wrapped);
    this.serverInstance = serverInstance;
    final int cl = Runtime.getRuntime().availableProcessors() * 4;
    distributedQueryExecutors = new ThreadPoolExecutor(0, cl, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(cl),
        new ThreadFactory() {

          private final AtomicInteger i = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            final Thread t = new Thread(Thread.currentThread().getThreadGroup(), r, "DistributedQueryExecutor-"
                + i.getAndIncrement());
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
          }
        });
  }

  /**
   * Run given command on all nodes.
   * 
   * @param iDistributedCommand
   *          command to execute
   * @return number of the nodes that are running command
   */
  protected int runCommandOnAllNodes(final OCommandRequestText iDistributedCommand) {
    final List<ODHTNode> nodes = serverInstance.getDHTNodes();
    final int nodesNumber = nodes.size();
    final List<Future> tasks = new ArrayList<Future>(nodesNumber);
    for (final ODHTNode node : nodes) {
      tasks.add(distributedQueryExecutors.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Object result = node.command(wrapped.getName(), iDistributedCommand, false);
            if (result != null && !node.isLocal()) {
              // generally we need thread local database for record deserializing, but not hear
              // select resultset will be routed thought OHazelcastResultListener, so it will never reach this block
              // other commands return primitive types so that thread local database instance is not required for deserializing
              result = OCommandResultSerializationHelper.readFromStream((byte[]) result);
            }
            addResult(result);
          } catch (IOException e) {
            OLogManager.instance().error(this, "Error deserializing result from node " + node.getNodeId(), e);
          }
        }
      }));
    }
    for (final Future task : tasks) {
      try {
        task.get();
      } catch (Exception e) {
        failedNodes.incrementAndGet();
        // OLogManager.instance().error(this, "Query execution failed on one of the nodes", e);
      }
    }
    return nodesNumber;
  }

  /**
   * Determine way to handle returned result
   * 
   * @param result
   */
  protected abstract void addResult(Object result);
}
