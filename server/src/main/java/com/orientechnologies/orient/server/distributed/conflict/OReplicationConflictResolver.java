/*
 * Copyright 2009-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OReplicationConflictResolver {
  public void startup(final OServer iServer, ODistributedServerManager iCluster, String iStorageName);

  public void shutdown();

  public ODocument getAllConflicts();

  public void handleUpdateConflict(String iRemoteNodeId, ORecordId iCurrentRID, ORecordVersion iCurrentVersion,
      ORecordVersion iOtherVersion);

  public void handleCreateConflict(String iRemoteNodeId, ORecordId iCurrentRID, ORecordId iOtherRID);

  public void handleDeleteConflict(String iRemoteNodeId, ORecordId iCurrentRID);

  public void handleCommandConflict(String iRemoteNodeId, Object iCommand, Object iLocalResult, Object iRemoteResult);

  public boolean existConflictsForRecord(final ORecordId iRID);
}
