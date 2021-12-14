/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Base class for Tasks to be executed remotely.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public abstract class OAbstractRemoteTask implements ORemoteTask {

  protected transient String nodeSource;

  /** Constructor used from unmarshalling. */
  public OAbstractRemoteTask() {}

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public long getSynchronousTimeout(final int iSynchNodes) {
    if (iSynchNodes <= 0) return getDistributedTimeout();

    return getDistributedTimeout() * iSynchNodes;
  }

  @Override
  public long getTotalTimeout(final int iTotalNodes) {
    if (iTotalNodes <= 0) return getDistributedTimeout();

    return getDistributedTimeout() * iTotalNodes;
  }

  @Override
  public boolean hasResponse() {
    return true;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.ANY;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public String getNodeSource() {
    return nodeSource;
  }

  @Override
  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return true;
  }

  @Override
  public boolean isUsingDatabase() {
    return true;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {}

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {}
}
