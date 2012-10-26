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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * Executor for distributed commands such as metadata manipulation.
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:12
 */
public class ODistributedQueryExecutor extends OAbstractDistributedQueryExecutor {

  private final Object resultLock = new Object();
  private Object       result;

  public ODistributedQueryExecutor(OCommandRequestText iCommand, OStorageEmbedded wrapped, ServerInstance serverInstance) {
    super(iCommand, wrapped, serverInstance);
  }

  @Override
  protected void addResult(Object result) {
    synchronized (resultLock) {
      if (this.result == null) {
        this.result = result;
      } else if (!this.result.equals(result)) {
        OLogManager.instance().warn(this, "One of the nodes has returned different result");
      }
    }
  }

  @Override
  public Object execute() {
    final int runs = runCommandOnAllNodes(iCommand);

    final int fails = failedNodes.get();
    if (fails > 0) {
      OLogManager.instance().warn(this, String.format("%d nodes of %d have failed during execution", fails, runs));
    }

    return result;
  }
}
