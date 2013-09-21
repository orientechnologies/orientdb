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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;

/**
 * Base class for Replicated tasks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractReplicatedTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  public OAbstractReplicatedTask() {
  }

  public abstract OAbstractReplicatedTask copy();

  public abstract String getPayload();

  /**
   * Handles conflict between local and remote execution results.
   * 
   * @param localResult
   *          The result on local node
   * @param remoteResult
   *          the result on remote node
   * @param remoteResult2
   */
  public void handleConflict(String iDatabaseName, final String iRemoteNode, Object localResult, Object remoteResult,
      OReplicationConflictResolver iConfictStrategy) {
  }

  @Override
  public OAbstractRemoteTask copy(final OAbstractRemoteTask iCopy) {
    super.copy(iCopy);
    return iCopy;
  }
}
