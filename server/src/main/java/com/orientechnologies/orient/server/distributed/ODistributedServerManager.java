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
package com.orientechnologies.orient.server.distributed;

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.distributed.ODistributedTask.OPERATION;

/**
 * Server cluster interface to abstract cluster behavior.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODistributedServerManager {

  public enum EXECUTION_MODE {
    SYNCHRONOUS, ASYNCHRONOUS, FIRE_AND_FORGET
  }

  public Object executeOperation(String iNodeId, OPERATION op, String dbName, ORecordId rid, int iVersion, ORawBuffer record,
      EXECUTION_MODE iMode) throws ODistributedException;

  public Collection<Object> executeOperation(Set<String> iNodeIds, OPERATION op, String dbName, ORecordId rid, int iVersion,
      ORawBuffer record, EXECUTION_MODE iMode) throws ODistributedException;

  public String getLocalNodeId();

  public Set<String> getRemoteNodeIds();

  public ODocument getDatabaseStatus(String iDatabaseName);

  public ODocument getDatabaseConfiguration(String iDatabaseName);

  public ODocument getClusterConfiguration();

  public ODocument getLocalNodeConfiguration();

  /**
   * Returns the offset in milliseconds as difference between the current date time and the central cluster time. This allows to
   * have a quite precise idea about information on date times, such as logs to determine the youngest in case of conflict.
   * 
   * @return
   */
  public long getTimeOffset();

  public OStorageSynchronizer getDatabaseSynchronizer(String iDatabaseName, String iNodeId);
}
