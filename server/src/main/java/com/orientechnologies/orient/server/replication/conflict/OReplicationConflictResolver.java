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
package com.orientechnologies.orient.server.replication.conflict;

import java.util.Map;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.OReplicator;

/**
 * Interface to handle conflicts on distributed replication.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OReplicationConflictResolver {

  public abstract void config(final OReplicator iReplicator, Map<String, String> iConfig);

  /**
   * Initializes the replication for a database
   */
  public abstract void init(ODatabaseComplex<?> iDatabase);

  public abstract OIdentifiable searchForConflict(OIdentifiable iRecord);

  public abstract ODocument getAllConflicts(ODatabaseRecord iDatabase);

  public void handleCreateConflict(byte iOperation, SYNCH_TYPE iRequestType, ORecordInternal<?> iRecord, long iOtherClusterPosition);

  public void handleUpdateConflict(byte iOperation, SYNCH_TYPE iRequestType, ORecordInternal<?> iRecord, int iCurrentVersion,
      int iOtherVersion);

  public void handleDeleteConflict(byte iOperation, SYNCH_TYPE iRequestType, ORecordInternal<?> iRecord);
}