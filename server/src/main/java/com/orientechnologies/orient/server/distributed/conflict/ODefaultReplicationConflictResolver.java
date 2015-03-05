/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.Date;
import java.util.List;

/**
 * Default conflict resolver.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @deprecated since 2.0
 */
@Deprecated
public class ODefaultReplicationConflictResolver implements OReplicationConflictResolver {

  private static final String       DISTRIBUTED_CONFLICT_CLASS = "ODistributedConflict";
  private static final String       FIELD_RECORD               = "record";
  private static final String       FIELD_NODE                 = "node";
  private static final String       FIELD_DATE                 = "date";
  private static final String       FIELD_OPERATION            = "operation";
  private static final String       FIELD_OTHER_RID            = "otherRID";
  private static final String       FIELD_CURRENT_VERSION      = "currentVersion";
  private static final String       FIELD_OTHER_VERSION        = "otherVersion";
  private static final int          MAX_RETRIES                = 10;

  private boolean                   ignoreIfSameContent;
  private boolean                   ignoreIfMergeOk;
  private boolean                   latestAlwaysWin;

  private ODatabase<?>              database;
  private OIndex<?>                 index                      = null;
  private OServer                   serverInstance;
  private ODistributedServerManager cluster;

  public ODefaultReplicationConflictResolver() {
  }

  public void startup(final OServer iServer, final ODistributedServerManager iCluster, final String iDatabaseName) {
    serverInstance = iServer;
    cluster = iCluster;

    synchronized (this) {
      if (index != null) {
          return;
      }

      final OServerUserConfiguration replicatorUser = serverInstance.getUser(ODistributedAbstractPlugin.REPLICATOR_USER);

      final ODatabaseDocumentInternal threadDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      if (threadDb != null && !threadDb.isClosed() && threadDb.getStorage().getName().equals(iDatabaseName)) {
          database = threadDb;
      } else {
          database = serverInstance.openDatabase("document", iDatabaseName, replicatorUser.name, replicatorUser.password);
      }

      if (database.getMetadata() != null) {
        OClass cls = database.getMetadata().getSchema().getClass(DISTRIBUTED_CONFLICT_CLASS);
        final OProperty p;
        if (cls == null) {
          cls = database.getMetadata().getSchema().createClass(DISTRIBUTED_CONFLICT_CLASS);
          index = cls.createProperty(FIELD_RECORD, OType.LINK).createIndex(INDEX_TYPE.UNIQUE);
        } else {
          p = cls.getProperty(FIELD_RECORD);
          if (p == null) {
              index = cls.createProperty(FIELD_RECORD, OType.LINK).createIndex(INDEX_TYPE.UNIQUE);
          } else {
            index = p.getIndex();
          }
        }
      }
    }
  }

  public void shutdown() {
    if (database != null) {
        database.close();
    }

    if (index != null) {
        index = null;
    }
  }

  @Override
  public void handleCreateConflict(final String iRemoteNode, final ORecordId iCurrentRID, final int iCurrentVersion,
      final ORecordId iOtherRID, final int iOtherVersion) {
    if (iCurrentRID.equals(iOtherRID)) {
      // PATCH FOR THE CASE THE RECORD WAS DELETED WHILE THE NODE WAS OFFLINE: FORCE THE OTHER VERSION TO THE LOCAL ONE (-1
      // BECAUSE IT'S ALWAYS INCREMENTED)
      for (int retry = 0; retry < MAX_RETRIES; ++retry) {
        ODistributedServerLog
            .debug(
                this,
                cluster.getLocalNodeName(),
                iRemoteNode,
                DIRECTION.IN,
                "Resolved conflict automatically between versions on CREATE record %s/%s v.%d (other RID=%s v.%d). Current record version will be overwritten",
                database.getName(), iCurrentRID, iCurrentVersion, iOtherRID, iOtherVersion);

        final ORecord record = iCurrentRID.getRecord();
        ORecordInternal.setVersion(record, iOtherVersion - 1);
        record.setDirty();

        try {
          record.save();
          return;
        } catch (OConcurrentModificationException e) {
          // CONCURRENT OPERATION, RETRY AGAIN?
        }
      }
    }

    ODistributedServerLog.warn(this, cluster.getLocalNodeName(), iRemoteNode, DIRECTION.IN,
        "Conflict on CREATE record %s/%s v.%d (other RID=%s v.%d)...", database.getName(), iCurrentRID, iCurrentVersion, iOtherRID,
        iOtherVersion);

    if (!existConflictsForRecord(iCurrentRID)) {
      final ODocument doc = createConflictDocument(ORecordOperation.CREATED, iCurrentRID, iRemoteNode);
      try {
        // WRITE THE CONFLICT AS RECORD
        doc.field(FIELD_OTHER_RID, iOtherRID);
        doc.save();
      } catch (Exception e) {
        errorOnWriteConflict(iRemoteNode, doc);
      }
    }
  }

  @Override
  public void handleUpdateConflict(final String iRemoteNode, final ORecordId iCurrentRID, final ORecordVersion iCurrentVersion,
      final ORecordVersion iOtherVersion) {
    final int otherVersionNumber = iOtherVersion == null ? -1 : iOtherVersion.getCounter();

    ODistributedServerLog.warn(this, cluster.getLocalNodeName(), iRemoteNode, DIRECTION.IN,
        "Conflict on UDPATE record %s/%s (current=v%d, other=v%d)...", database.getName(), iCurrentRID,
        iCurrentVersion.getCounter(), otherVersionNumber);

    if (!existConflictsForRecord(iCurrentRID)) {
      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(ORecordOperation.UPDATED, iCurrentRID, iRemoteNode);
      try {
        doc.field(FIELD_CURRENT_VERSION, iCurrentVersion.getCounter());
        doc.field(FIELD_OTHER_VERSION, otherVersionNumber);
        doc.save();
      } catch (Exception e) {
        errorOnWriteConflict(iRemoteNode, doc);
      }
    }
  }

  @Override
  public void handleDeleteConflict(final String iRemoteNode, final ORecordId iCurrentRID) {
    ODistributedServerLog.warn(this, cluster.getLocalNodeName(), iRemoteNode, DIRECTION.IN,
        "Conflict on DELETE record %s/%s (cannot be deleted on other node)", database.getName(), iCurrentRID);

    if (!existConflictsForRecord(iCurrentRID)) {
      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(ORecordOperation.DELETED, iCurrentRID, iRemoteNode);
      try {
        doc.save();
      } catch (Exception e) {
        errorOnWriteConflict(iRemoteNode, doc);
      }
    }
  }

  @Override
  public void handleCommandConflict(final String iRemoteNode, final Object iCommand, Object iLocalResult, Object iRemoteResult) {
    ODistributedServerLog.warn(this, cluster.getLocalNodeName(), iRemoteNode, DIRECTION.IN,
        "Conflict on COMMAND execution on db '%s', cmd='%s' result local=%s, remote=%s", database.getName(), iCommand,
        iLocalResult, iRemoteResult);
  }

  @Override
  public ODocument getAllConflicts() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseDocumentInternal) database);
    final List<OIdentifiable> entries = database.query(new OSQLSynchQuery<OIdentifiable>("select from "
        + DISTRIBUTED_CONFLICT_CLASS));

    // EARLY LOAD CONTENT
    final ODocument result = new ODocument().field("result", entries);
    for (int i = 0; i < entries.size(); ++i) {
      final ODocument record = entries.get(i).getRecord();
      record.setClassName(null);
      ODocumentInternal.addOwner(record, result);
      record.getIdentity().reset();

      final Byte operation = record.field("operation");
      record.field("operation", ORecordOperation.getName(operation));

      entries.set(i, record);
    }
    return result;
  }

  @Override
  public ODocument reset() {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseDocumentInternal) database);
    final int deleted = (Integer) database.command(new OSQLSynchQuery<OIdentifiable>("delete from " + DISTRIBUTED_CONFLICT_CLASS))
        .execute();
    return new ODocument().field("result", deleted);
  }

  /**
   * Searches for a conflict by RID.
   *
   * @param iRID
   *          RID to search
   */
  public boolean existConflictsForRecord(final ORecordId iRID) {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseDocumentInternal) database);
    if (index == null) {
      ODistributedServerLog.warn(this, cluster.getLocalNodeName(), null, DIRECTION.NONE,
          "Index against %s is not available right now, searches will be slower", DISTRIBUTED_CONFLICT_CLASS);

      final List<?> result = database.query(new OSQLSynchQuery<Object>("select from " + DISTRIBUTED_CONFLICT_CLASS + " where "
          + FIELD_RECORD + " = " + iRID.toString()));
      return !result.isEmpty();
    }

    if (index.contains(iRID)) {
      ODistributedServerLog.info(this, cluster.getLocalNodeName(), null, DIRECTION.NONE,
          "Conflict already present for record %s, skip it", iRID);
      return true;
    }
    return false;
  }

  protected ODocument createConflictDocument(final byte iOperation, final ORecordId iRid, final String iServerNode) {
    ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseDocumentInternal) database);

    final ODocument doc = new ODocument(DISTRIBUTED_CONFLICT_CLASS);
    doc.field(FIELD_OPERATION, iOperation);
    doc.field(FIELD_DATE, new Date());
    doc.field(FIELD_RECORD, iRid);
    doc.field(FIELD_NODE, iServerNode);
    return doc;
  }

  protected void errorOnWriteConflict(final String iRemoteNode, final ODocument doc) {
    ODistributedServerLog.error(this, cluster.getLocalNodeName(), iRemoteNode, DIRECTION.IN,
        "Error on saving CONFLICT for record %s/%s...", database.getName(), doc);
  }

}
