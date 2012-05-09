/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.OReplicator;

/**
 * Default conflict resolver.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODefaultReplicationConflictResolver implements OReplicationConflictResolver {

  public static final String   DISTRIBUTED_CONFLICT_CLASS = "ODistributedConflict";
  private static final String  FIELD_RECORD               = "record";
  private static final String  FIELD_DATE                 = "date";
  private static final String  FIELD_OPERATION            = "operation";
  private static final String  FIELD_OTHER_CLUSTER_POS    = "otherClusterPos";
  private static final String  FIELD_OTHER_VERSION        = "otherVersion";
  private static final String  FIELD_CURRENT_VERSION      = "currentVersion";

  private OReplicator          replicator;
  private final OClusterLogger logger                     = new OClusterLogger();

  private boolean              ignoreIfSameContent;
  private boolean              ignoreIfMergeOk;
  private boolean              latestAlwaysWin;
  private OIndex<?>            index                      = null;

  public void config(final OReplicator iReplicator, final Map<String, String> iConfig) {
    replicator = iReplicator;
    replicator.addIgnoredDocumentClass(DISTRIBUTED_CONFLICT_CLASS);
    replicator.addIgnoredCluster(OStorageLocal.CLUSTER_INTERNAL_NAME);
    replicator.addIgnoredCluster(OStorageLocal.CLUSTER_INDEX_NAME);

    ignoreIfSameContent = Boolean.parseBoolean(iConfig.get("ignoreIfSameContent"));
    ignoreIfMergeOk = Boolean.parseBoolean(iConfig.get("ignoreIfMergeOk"));
    latestAlwaysWin = Boolean.parseBoolean(iConfig.get("latestAlwaysWin"));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleCreateConflict(byte,
   * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
   * com.orientechnologies.orient.core.record.ORecordInternal, long)
   */
  @Override
  public void handleCreateConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord,
      final long iOtherClusterPosition) {
    logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.IN, "CONFLICT %s record %s (other RID=#%d:%d)...",
        ORecordOperation.getName(iOperation), iRecord.getIdentity(), iRecord.getIdentity().getClusterId(), iOtherClusterPosition);

    if (searchForConflict(iRecord) != null) {
      logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.IN, "Conflict already present for record %s, skip it",
          iRecord.getIdentity());
    } else {

      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(iOperation, iRecord);
      doc.field(FIELD_OTHER_CLUSTER_POS, iOtherClusterPosition);
      doc.save();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleUpdateConflict(byte,
   * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
   * com.orientechnologies.orient.core.record.ORecordInternal, int, int)
   */
  @Override
  public void handleUpdateConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord,
      final int iCurrentVersion, final int iOtherVersion) {
    logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.IN, "CONFLICT %s record %s (current=v%d, other=v%d)...",
        ORecordOperation.getName(iOperation), iRecord.getIdentity(), iCurrentVersion, iOtherVersion);

    if (searchForConflict(iRecord) != null) {
      logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.IN, "Conflict already present for record %s, skip it",
          iRecord.getIdentity());
    } else {

      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(iOperation, iRecord);
      doc.field(FIELD_CURRENT_VERSION, iCurrentVersion);
      doc.field(FIELD_OTHER_VERSION, iOtherVersion);
      doc.save();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleDeleteConflict(byte,
   * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
   * com.orientechnologies.orient.core.record.ORecordInternal)
   */
  @Override
  public void handleDeleteConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord) {
    logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.IN,
        "-> %s (%s mode) CONFLICT %s record %s (cannot be deleted on other node)", ORecordOperation.getName(iOperation),
        iRecord.getIdentity());

    if (searchForConflict(iRecord) != null) {
      logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.IN, "Conflict already present for record %s, skip it",
          iRecord.getIdentity());
    } else {

      // WRITE THE CONFLICT AS RECORD
      final ODocument doc = createConflictDocument(iOperation, iRecord);
      doc.save();
    }
  }

  @Override
  public ODocument getAllConflicts(final ODatabaseRecord iDatabase) {
    final List<OIdentifiable> entries = iDatabase.query(new OSQLSynchQuery<OIdentifiable>("select from "
        + DISTRIBUTED_CONFLICT_CLASS));

    // EARLY LOAD CONTENT
    final ODocument result = new ODocument().field("entries", entries);
    for (int i = 0; i < entries.size(); ++i) {
      entries.set(i, entries.get(i).getRecord());
    }
    return result;
  }

  /**
   * Searches for a conflict by record.
   * 
   * @param iRecord
   *          RID to search
   * @return The document if any, otherwise null
   */
  public OIdentifiable searchForConflict(final OIdentifiable iRecord) {
    init();
    return (OIdentifiable) index.get(iRecord);
  }

  protected ODocument createConflictDocument(final byte iOperation, final ORecordInternal<?> iRecord) {
    init();
    final ODocument doc = new ODocument(DISTRIBUTED_CONFLICT_CLASS);
    doc.field(FIELD_OPERATION, iOperation);
    doc.field(FIELD_DATE, new Date());
    doc.field(FIELD_RECORD, iRecord.getIdentity());
    return doc;
  }

  @Override
  public String toString() {
    return replicator.getManager().getId();
  }

  protected void init() {
    synchronized (this) {
      OClass cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(DISTRIBUTED_CONFLICT_CLASS);
      final OProperty p;
      if (cls == null) {
        cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().createClass(DISTRIBUTED_CONFLICT_CLASS);
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
}
