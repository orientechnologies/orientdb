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
package com.orientechnologies.orient.server.hazelcast.sharding.hazelcast;

import java.io.IOException;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.server.hazelcast.sharding.OCommandResultSerializationHelper;
import com.orientechnologies.orient.server.hazelcast.sharding.ODistributedSelectQueryExecutor;

/**
 * This class provides functionality to async aggregation of result sets from different nodes to one that initiated query
 * 
 * @author edegtyarenko
 * @since 22.10.12 11:53
 */
public class OHazelcastResultListener implements OCommandResultListener {

  public static final class EndOfResult {

    private final long nodeId;

    public EndOfResult(long nodeId) {
      this.nodeId = nodeId;
    }

    public long getNodeId() {
      return nodeId;
    }
  }

  private final long           storageId;
  private final long           selectId;
  private final ITopic<byte[]> topic;

  public OHazelcastResultListener(HazelcastInstance hazelcast, long storageId, long selectId) {
    this.storageId = storageId;
    this.selectId = selectId;
    this.topic = hazelcast.getTopic(ODistributedSelectQueryExecutor.getResultTopicName(storageId, selectId));
  }

  @Override
  public boolean result(Object iRecord) {
    try {
      topic.publish(OCommandResultSerializationHelper.writeToStream(iRecord));
      return true;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error serializing record", e);
      return false;
    }
  }

  public long getStorageId() {
    return storageId;
  }

  public long getSelectId() {
    return selectId;
  }
}
