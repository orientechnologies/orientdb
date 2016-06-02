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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Synchronizes a database from the wire.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OIncrementalServerSync {

  private static final byte[] EMPTY_CONTENT = new byte[0];

  /**
   * Deleted records are written in output stream first, then created/updated records. All records are sorted by record id.
   * <p>
   * Each record in output stream is written using following format:
   * <ol>
   * <li>Record's cluster id - 4 bytes</li>
   * <li>Record's cluster position - 8 bytes</li>
   * <li>Delete flag, 1 if record is deleted - 1 byte</li>
   * <li>Record version , only if record is not deleted - 4 bytes</li>
   * <li>Record type, only if record is not deleted - 1 byte</li>
   * <li>Length of binary presentation of record, only if record is not deleted - 4 bytes</li>
   * <li>Binary presentation of the record, only if record is not deleted - length of content is provided in above entity</li>
   * </ol>
   */
  public void importDelta(final OServer serverInstance, final ODatabaseDocumentInternal db, final FileInputStream in, final String iNode)
      throws IOException {
    final String nodeName = serverInstance.getDistributedManager().getLocalNodeName();

    try {
      serverInstance.openDatabase(db);

      OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          db.activateOnCurrentThread();

          long totalRecords = 0;
          long totalCreated = 0;
          long totalUpdated = 0;
          long totalDeleted = 0;
          long totalHoles = 0;
          long totalSkipped = 0;

          ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN,
              "Started import of delta for database '" + db.getName() + "'");

          long lastLap = System.currentTimeMillis();

          // final GZIPInputStream gzipInput = new GZIPInputStream(in);
          try {

            final DataInputStream input = new DataInputStream(in);
            try {

              final long records = input.readLong();

              for (long i = 0; i < records; ++i) {
                final int clusterId = input.readInt();
                final long clusterPos = input.readLong();
                final boolean deleted = input.readBoolean();

                final ORecordId rid = new ORecordId(clusterId, clusterPos);

                totalRecords++;

                final OPaginatedCluster cluster = (OPaginatedCluster) db.getStorage().getUnderlying().getClusterById(rid.clusterId);
                final OPaginatedCluster.RECORD_STATUS recordStatus = cluster.getRecordStatus(rid.clusterPosition);

                ORecord newRecord = null;

                if (deleted) {
                  ODistributedServerLog.debug(this, nodeName, iNode, DIRECTION.IN, "DELTA <- deleting %s", rid);

                  switch (recordStatus) {
                  case REMOVED:
                    // SKIP IT
                    totalSkipped++;
                    continue;

                  case ALLOCATED:
                  case PRESENT:
                    // DELETE IT
                    db.delete(rid);
                    break;

                  case NOT_EXISTENT:
                    totalSkipped++;
                    break;
                  }

                  totalDeleted++;

                } else {
                  final int recordVersion = input.readInt();
                  final int recordType = input.readByte();
                  final int recordSize = input.readInt();
                  final byte[] recordContent = new byte[recordSize];
                  input.read(recordContent);

                  switch (recordStatus) {
                  case REMOVED:
                    // SKIP IT
                    totalSkipped++;
                    continue;

                  case ALLOCATED:
                  case PRESENT:
                    // UPDATE IT
                    newRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) recordType);
                    ORecordInternal.fill(newRecord, rid, ORecordVersionHelper.setRollbackMode(recordVersion), recordContent, true);

                    final ORecord loadedRecord = rid.getRecord();
                    if (loadedRecord instanceof ODocument) {
                      // APPLY CHANGES FIELD BY FIELD TO MARK DIRTY FIELDS FOR INDEXES/HOOKS
                      ODocument loadedDocument = (ODocument) loadedRecord;
                      loadedDocument.merge((ODocument) newRecord, false, false).getVersion();
                      loadedDocument.setDirty();
                      newRecord = loadedDocument;
                    }

                    // SAVE THE UPDATE RECORD
                    newRecord.save();

                    ODistributedServerLog.debug(this, nodeName, iNode, DIRECTION.IN,
                        "DELTA <- updating rid=%s type=%d size=%d v=%d content=%s", rid, recordType, recordSize, recordVersion,
                        newRecord);

                    totalUpdated++;
                    break;

                  case NOT_EXISTENT:
                    // CREATE AND DELETE RECORD IF NEEDED
                    do {
                      newRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) recordType);
                      ORecordInternal.fill(newRecord, new ORecordId(rid.getClusterId(), -1), recordVersion - 1, recordContent,
                          true);

                      try {
                        newRecord.save();
                      } catch (ORecordNotFoundException e) {
                        ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN,
                            "DELTA <- error on saving record (not found) rid=%s type=%d size=%d v=%d content=%s", rid, recordType,
                            recordSize, recordVersion, newRecord);
                      } catch (ORecordDuplicatedException e) {
                        ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN,
                            "DELTA <- error on saving record (duplicated %s) rid=%s type=%d size=%d v=%d content=%s", e.getRid(),
                            rid, recordType, recordSize, recordVersion, newRecord);
                        // throw OException.wrapException(
                        // new ODistributedDatabaseDeltaSyncException("Error on delta sync: found duplicated record " + rid), e);

                        final ORecord duplicatedRecord = db.load(e.getRid(), null, true);
                        if (duplicatedRecord == null) {
                          // RECORD REMOVED: THE INDEX IS DIRTY, FIX THE DIRTY INDEX
                          final ODocument doc = (ODocument) newRecord;
                          final OIndex<?> index = db.getMetadata().getIndexManager().getIndex(e.getIndexName());
                          final List<String> fields = index.getDefinition().getFields();
                          final List<Object> values = new ArrayList<Object>(fields.size());
                          for (String f : fields) {
                            values.add(doc.field(f));
                          }
                          final Object keyValue = index.getDefinition().createValue(values);
                          index.remove(keyValue, e.getRid());

                          // RESAVE THE RECORD
                          newRecord.save();
                        } else
                          break;
                      }

                      if (newRecord.getIdentity().getClusterPosition() < clusterPos) {
                        // DELETE THE RECORD TO CREATE A HOLE
                        ODistributedServerLog.debug(this, nodeName, iNode, DIRECTION.IN, "DELTA <- creating hole rid=%s",
                            newRecord.getIdentity());
                        newRecord.delete();
                        totalHoles++;
                      }

                    } while (newRecord.getIdentity().getClusterPosition() < clusterPos);

                    ODistributedServerLog.debug(this, nodeName, iNode, DIRECTION.IN,
                        "DELTA <- creating rid=%s type=%d size=%d v=%d content=%s", rid, recordType, recordSize, recordVersion,
                        newRecord);

                    totalCreated++;
                    break;
                  }

                  if (newRecord.getIdentity().isPersistent() && !newRecord.getIdentity().equals(rid))
                    throw new ODistributedDatabaseDeltaSyncException(
                        "Error on synchronization of records, rids are different: saved " + newRecord.getIdentity()
                            + ", but it should be " + rid);
                }

                final long now = System.currentTimeMillis();
                if (now - lastLap > 2000) {
                  // DUMP STATS EVERY SECOND
                  ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN,
                      "- %,d total entries: %,d created, %,d updated, %,d deleted, %,d holes, %,d skipped...", totalRecords,
                      totalCreated, totalUpdated, totalDeleted, totalHoles, totalSkipped);
                  lastLap = now;
                }
              }

              db.getMetadata().reload();

            } finally {
              input.close();
            }

          } catch (Exception e) {
            ODistributedServerLog.error(this, nodeName, iNode, DIRECTION.IN,
                "Error on installing database delta '%s' on local server", e, db.getName());
            throw OException.wrapException(
                new ODistributedException("Error on installing database delta '" + db.getName() + "' on local server"), e);
          } finally {
            // gzipInput.close();
          }

          ODistributedServerLog.info(this, nodeName, iNode, DIRECTION.IN,
              "Installed database delta for '%s'. %d total entries: %d created, %d updated, %d deleted, %d holes, %,d skipped",
              db.getName(), totalRecords, totalCreated, totalUpdated, totalDeleted, totalHoles, totalSkipped);

          return null;
        }
      });

      db.activateOnCurrentThread();

    } catch (Exception e) {
      // FORCE FULL DATABASE SYNC
      ODistributedServerLog.error(this, nodeName, iNode, DIRECTION.IN,
          "Error while applying changes of database delta sync on '%s': forcing full database sync...", e, db.getName());
      throw OException
          .wrapException(
              new ODistributedDatabaseDeltaSyncException(
                  "Error while applying changes of database delta sync on '" + db.getName() + "': forcing full database sync..."),
              e);
    }
  }
}
