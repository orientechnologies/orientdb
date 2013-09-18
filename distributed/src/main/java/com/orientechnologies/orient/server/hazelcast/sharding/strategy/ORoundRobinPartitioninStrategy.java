/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.hazelcast.sharding.strategy;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedPartition;
import com.orientechnologies.orient.server.distributed.ODistributedPartitioningStrategy;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Interface that represents the replication strategy.
 * 
 * @author luca
 * 
 */
public class ORoundRobinPartitioninStrategy implements ODistributedPartitioningStrategy {
  protected ConcurrentHashMap<String, AtomicLong> lastPartition = new ConcurrentHashMap<String, AtomicLong>();

  @Override
  public ODistributedPartition getPartition(final ODistributedServerManager iManager, final String iDatabaseName,
      final String iClusterName) {

    final ODistributedConfiguration cfg = iManager.getDatabaseConfiguration(iDatabaseName);
    final List<List<String>> partitions = cfg.getPartitions(iClusterName);

    final List<String> partition;
    if (partitions.size() > 1) {
      // APPLY ROUND ROBIN
      AtomicLong lastPos = lastPartition.get(iDatabaseName);
      if (lastPos == null) {
        lastPos = new AtomicLong(-1l);
        lastPartition.putIfAbsent(iDatabaseName, lastPos);
      }

      final long newPos = lastPos.incrementAndGet();

      partition = partitions.get((int) (newPos % partitions.size()));
    } else
      // ONLY ONE PARTITION: JUST USE IT
      partition = partitions.get(0);

    return iManager.newPartition(partition);
  }
}
