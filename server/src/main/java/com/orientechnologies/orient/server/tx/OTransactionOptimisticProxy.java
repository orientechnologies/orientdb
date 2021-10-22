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
package com.orientechnologies.orient.server.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRecordIndexOperation;
import java.util.*;
import java.util.Map.Entry;

public class OTransactionOptimisticProxy extends OTransactionOptimistic {
  private final Map<ORID, ORecordOperation> tempEntries =
      new LinkedHashMap<ORID, ORecordOperation>();
  private final Map<ORecordId, ORecord> createdRecords = new HashMap<ORecordId, ORecord>();
  private final Map<ORecordId, ORecord> updatedRecords = new HashMap<ORecordId, ORecord>();
  @Deprecated private final int clientTxId;
  private final short protocolVersion;
  private List<ORecordOperationRequest> operations;
  private final ODocument indexChanges;
  private final ORecordSerializer serializer;

  public OTransactionOptimisticProxy(
      ODatabaseDocumentInternal database,
      int txId,
      boolean usingLong,
      List<ORecordOperationRequest> operations,
      ODocument indexChanges,
      short protocolVersion,
      ORecordSerializer serializer) {
    super(database);
    clientTxId = id;
    setUsingLog(usingLong);
    this.operations = operations;
    this.indexChanges = indexChanges;
    this.protocolVersion = protocolVersion;
    this.serializer = serializer;
  }

  @Override
  public void begin() {
    super.begin();
    try {
      for (ORecordOperationRequest operation : this.operations) {
        final byte recordStatus = operation.getType();

        final ORecordId rid = (ORecordId) operation.getId();

        final ORecordOperation entry;

        switch (recordStatus) {
          case ORecordOperation.CREATED:
            ORecord record =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(operation.getRecordType(), rid.getClusterId(), getDatabase());
            serializer.fromStream(operation.getRecord(), record, null);
            ORecordInternal.setIdentity(record, rid);
            ORecordInternal.setVersion(record, 0);
            entry = new ORecordOperation(record, ORecordOperation.CREATED);
            record.setDirty();

            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
            createdRecords.put(rid.copy(), entry.getRecord());
            break;

          case ORecordOperation.UPDATED:
            int version = operation.getVersion();
            ORecord updated =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(operation.getRecordType(), rid.getClusterId(), getDatabase());
            ORecordInternal.setIdentity(updated, rid);
            ORecordInternal.setVersion(updated, version);
            entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
            updated.setDirty();
            ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
            break;

          case ORecordOperation.DELETED:
            // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
            final ORecord rec = rid.getRecord();
            entry = new ORecordOperation(rec, ORecordOperation.DELETED);
            int deleteVersion = operation.getVersion();
            if (rec == null) throw new ORecordNotFoundException(rid.getIdentity());
            else {
              ORecordInternal.setVersion(rec, deleteVersion);
              entry.setRecord(rec);
            }
            break;

          default:
            throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }
      this.operations = null;

      final ODocument remoteIndexEntries = indexChanges;
      fillIndexOperations(remoteIndexEntries);

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {

        if (entry.getValue().type == ORecordOperation.UPDATED) {
          // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING
          // HOOKS (LIKE INDEXES)
          final ORecord record = entry.getValue().record.getRecord();
          final boolean contentChanged = ORecordInternal.isContentChanged(record);

          final ORecord loadedRecord = record.getIdentity().copy().getRecord();
          if (loadedRecord == null) throw new ORecordNotFoundException(record.getIdentity());

          if (ORecordInternal.getRecordType(loadedRecord) == ODocument.RECORD_TYPE
              && ORecordInternal.getRecordType(loadedRecord)
                  == ORecordInternal.getRecordType(record)) {
            ((ODocument) loadedRecord).merge((ODocument) record, false, false);

            loadedRecord.setDirty();
            ORecordInternal.setContentChanged(loadedRecord, contentChanged);

            ORecordInternal.setVersion(loadedRecord, record.getVersion());
            entry.getValue().record = loadedRecord;

            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
            updatedRecords.put((ORecordId) entry.getKey(), entry.getValue().getRecord());
          }
        }

        addRecord(entry.getValue().getRecord(), entry.getValue().type, null);
      }
      tempEntries.clear();

      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord record : createdRecords.values()) {
        unmarshallRecord(record);
        if (record instanceof ODocument) {
          // Force conversion of value to class for trigger default values.
          ODocumentInternal.autoConvertValueToClass(getDatabase(), (ODocument) record);
        }
      }
      for (ORecord record : updatedRecords.values()) unmarshallRecord(record);

    } catch (Exception e) {
      rollback();
      throw OException.wrapException(
          new OSerializationException(
              "Cannot read transaction record from the network. Transaction aborted"),
          e);
    }
  }

  @Override
  public ORecord getRecord(final ORID rid) {
    ORecord record = super.getRecord(rid);
    if (record == OTransactionAbstract.DELETED_RECORD) return record;
    else if (record == null && rid.isNew())
      // SEARCH BETWEEN CREATED RECORDS
      record = createdRecords.get(rid);

    return record;
  }

  private void fillIndexOperations(final ODocument remoteIndexEntries) {
    for (Entry<String, Object> indexEntry : remoteIndexEntries) {
      final String indexName = indexEntry.getKey();
      final ODocument indexDoc = (ODocument) indexEntry.getValue();
      if (indexDoc == null) continue;

      OTransactionIndexChanges transactionIndexChanges = indexEntries.get(indexEntry.getKey());

      if (transactionIndexChanges == null) {
        transactionIndexChanges = new OTransactionIndexChanges();
        indexEntries.put(indexEntry.getKey(), transactionIndexChanges);
      }

      final Boolean clearAll = indexDoc.field("clear");
      if (clearAll != null && clearAll) transactionIndexChanges.setCleared();

      final Collection<ODocument> entries = indexDoc.field("entries");
      if (entries == null) continue;

      for (final ODocument entry : entries) {
        final List<ODocument> operations = entry.field("ops");
        if (operations == null) continue;

        final Object key;
        ODocument keyContainer = entry.field("k");
        if (keyContainer != null) {
          final Object storedKey = keyContainer.field("key");
          if (storedKey instanceof List)
            key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
          else key = storedKey;
        } else key = null;

        for (final ODocument op : operations) {
          final int operation = op.rawField("o");
          final OTransactionIndexChanges.OPERATION indexOperation =
              OTransactionIndexChanges.OPERATION.values()[operation];
          final OIdentifiable value = op.field("v");

          transactionIndexChanges.getChangesPerKey(key).add(value, indexOperation);

          if (value == null) continue;

          final ORID rid = value.getIdentity();
          List<OTransactionRecordIndexOperation> txIndexOperations = recordIndexOperations.get(rid);
          if (txIndexOperations == null) {
            txIndexOperations = new ArrayList<OTransactionRecordIndexOperation>();
            recordIndexOperations.put(rid, txIndexOperations);
          }

          txIndexOperations.add(
              new OTransactionRecordIndexOperation(indexName, key, indexOperation));
        }
      }
    }
  }

  public Map<ORecordId, ORecord> getCreatedRecords() {
    return createdRecords;
  }

  public Map<ORecordId, ORecord> getUpdatedRecords() {
    return updatedRecords;
  }

  /** Unmarshalls collections. This prevent temporary RIDs remains stored as are. */
  private void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();
    }
  }
}
