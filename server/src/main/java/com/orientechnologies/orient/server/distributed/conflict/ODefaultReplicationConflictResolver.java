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
package com.orientechnologies.orient.server.distributed.conflict;

import java.util.Date;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Default conflict resolver.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODefaultReplicationConflictResolver implements OReplicationConflictResolver {

  private static final String DISTRIBUTED_CONFLICT_CLASS = "ODistributedConflict";
  private static final String FIELD_RECORD               = "record";
  private static final String FIELD_NODE                 = "node";
  private static final String FIELD_DATE                 = "date";
  private static final String FIELD_OPERATION            = "operation";
  private static final String FIELD_OTHER_RID            = "otherRID";
  private static final String FIELD_CURRENT_VERSION      = "currentVersion";
  private static final String FIELD_OTHER_VERSION        = "otherVersion";

  private boolean             ignoreIfSameContent;
  private boolean             ignoreIfMergeOk;
  private boolean             latestAlwaysWin;

  private ODatabaseComplex<?> database;
  private OIndex<?>           index                      = null;

  public ODefaultReplicationConflictResolver() {
  }

  public void startup(final ODistributedServerManager iDManager, final String iDatabaseName) {
    synchronized (this) {
      if (index != null)
        return;

      final OServerUserConfiguration replicatorUser = OServerMain.server().getUser(ODistributedAbstractPlugin.REPLICATOR_USER);

      final ODatabaseRecord threadDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      if (threadDb != null && threadDb.getStorage().getName().equals(iDatabaseName))
        database = threadDb;
      else
        database = OServerMain.server().openDatabase("document", iDatabaseName, replicatorUser.name, replicatorUser.password);

      OClass cls = database.getMetadata().getSchema().getClass(DISTRIBUTED_CONFLICT_CLASS);
      final OProperty p;
      if (cls == null) {
        cls = database.getMetadata().getSchema().createClass(DISTRIBUTED_CONFLICT_CLASS);
        index = cls.createProperty(FIELD_RECORD, OType.LINK).createIndex(INDEX_TYPE.UNIQUE);
      } else {
        p = cls.getProperty(FIELD_RECORD);
        if (p == null)
          index = cls.createProperty(FIELD_RECORD, OType.LINK).createIndex(INDEX_TYPE.UNIQUE);
        else {
          index = p.getIndex();
        }
      }
    }
  }

  public void shutdown() {
    if (database != null)
      database.close();

    if (index != null)
      index = null;
  }

  @Override
  public void handleCreateConflict(final String iRemoteNode, final ORecordId iCurrentRID, final ORecordId iOtherRID) {
    OLogManager.instance().warn(this, "CONFLICT against node %s CREATE record %s (other RID=%s)...", iRemoteNode, iCurrentRID,
        iOtherRID);

    if (!existConflictsForRecord(iCurrentRID)) {
      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(ORecordOperation.CREATED, iCurrentRID, iRemoteNode);
      doc.field(FIELD_OTHER_RID, iOtherRID);
      doc.save();
    }
  }

  @Override
  public void handleUpdateConflict(final String iRemoteNode, final ORecordId iCurrentRID, final ORecordVersion iCurrentVersion,
      final ORecordVersion iOtherVersion) {
    OLogManager.instance().warn(this, "CONFLICT against node %s UDPATE record %s (current=v%d, other=v%d)...", iRemoteNode,
        iCurrentRID, iCurrentVersion.getCounter(), iOtherVersion.getCounter());

    if (!existConflictsForRecord(iCurrentRID)) {
      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(ORecordOperation.UPDATED, iCurrentRID, iRemoteNode);
      doc.field(FIELD_CURRENT_VERSION, iCurrentVersion.getCounter());
      doc.field(FIELD_OTHER_VERSION, iOtherVersion.getCounter());
      doc.save();
    }
  }

  @Override
  public void handleDeleteConflict(final String iRemoteNode, final ORecordId iCurrentRID) {
    OLogManager.instance().warn(this, "CONFLICT against node %s DELETE record %s (cannot be deleted on other node)", iRemoteNode,
        iCurrentRID);

    if (!existConflictsForRecord(iCurrentRID)) {
      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(ORecordOperation.DELETED, iCurrentRID, iRemoteNode);
      doc.save();
    }
  }

  @Override
  public void handleCommandConflict(final String iRemoteNode, OCommandRequest iCommand, Object iLocalResult, Object iRemoteResult) {
    OLogManager.instance().warn(this, "CONFLICT against node %s COMMAND execution %s result local=%s, remote=%s", iRemoteNode,
        iCommand, iLocalResult, iRemoteResult);
  }

  @Override
  public ODocument getAllConflicts() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecord) database);
    final List<OIdentifiable> entries = database.query(new OSQLSynchQuery<OIdentifiable>("select from "
        + DISTRIBUTED_CONFLICT_CLASS));

    // EARLY LOAD CONTENT
    final ODocument result = new ODocument().field("entries", entries);
    for (int i = 0; i < entries.size(); ++i) {
      final ODocument record = entries.get(i).getRecord();
      record.setClassName(null);
      record.addOwner(result);
      record.getIdentity().reset();
      entries.set(i, record);
    }
    return result;
  }

  /**
   * Searches for a conflict by RID.
   * 
   * @param iRID
   *          RID to search
   */
  public boolean existConflictsForRecord(final ORecordId iRID) {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecord) database);
    if (index == null) {
      OLogManager.instance().warn(this, "Index against %s is not available right now, searches will be slower",
          DISTRIBUTED_CONFLICT_CLASS);

      final List<?> result = database.query(new OSQLSynchQuery<Object>("select from " + DISTRIBUTED_CONFLICT_CLASS + " where "
          + FIELD_RECORD + " = " + iRID.toString()));
      return !result.isEmpty();
    }

    if (index.contains(iRID)) {
      OLogManager.instance().info(this, "Conflict already present for record %s, skip it", iRID);
      return true;
    }
    return false;
  }

  protected ODocument createConflictDocument(final byte iOperation, final ORecordId iRid, final String iServerNode) {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecord) database);

    final ODocument doc = new ODocument(DISTRIBUTED_CONFLICT_CLASS);
    doc.field(FIELD_OPERATION, iOperation);
    doc.field(FIELD_DATE, new Date());
    doc.field(FIELD_RECORD, iRid);
    doc.field(FIELD_NODE, iServerNode);
    return doc;
  }
}
