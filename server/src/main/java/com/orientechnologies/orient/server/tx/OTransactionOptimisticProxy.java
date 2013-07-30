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
package com.orientechnologies.orient.server.tx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionAbortedException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OTransactionOptimisticProxy extends OTransactionOptimistic {
  private final Map<ORID, ORecordOperation>        tempEntries    = new LinkedHashMap<ORID, ORecordOperation>();
  private final Map<ORecordId, ORecordInternal<?>> createdRecords = new HashMap<ORecordId, ORecordInternal<?>>();
  private final Map<ORecordId, ORecordInternal<?>> updatedRecords = new HashMap<ORecordId, ORecordInternal<?>>();
  private final int                                clientTxId;
  private final OChannelBinary                     channel;

  public OTransactionOptimisticProxy(final ODatabaseRecordTx iDatabase, final OChannelBinary iChannel) throws IOException {
    super(iDatabase);
    channel = iChannel;
    clientTxId = iChannel.readInt();
  }

  @Override
  public void begin() {
    super.begin();

    try {
      setUsingLog(channel.readByte() == 1);

      byte lastTxStatus;
      for (lastTxStatus = channel.readByte(); lastTxStatus == 1; lastTxStatus = channel.readByte()) {
        final byte recordStatus = channel.readByte();

        final ORecordId rid = channel.readRID();

        final byte recordType = channel.readByte();
        final ORecordOperation entry = new OTransactionEntryProxy(recordType);
        entry.type = recordStatus;

        switch (recordStatus) {
        case ORecordOperation.CREATED:
          entry.getRecord().fill(rid, OVersionFactory.instance().createVersion(), channel.readBytes(), true);

          // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
          createdRecords.put(rid.copy(), entry.getRecord());
          break;

        case ORecordOperation.UPDATED:
          entry.getRecord().fill(rid, channel.readVersion(), channel.readBytes(), true);

          break;

        case ORecordOperation.DELETED:
          entry.getRecord().fill(rid, channel.readVersion(), null, false);
          break;

        default:
          throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }

      if (lastTxStatus == -1)
        // ABORT TX
        throw new OTransactionAbortedException("Transaction aborted by the client");

      final ODocument remoteIndexEntries = new ODocument(channel.readBytes());
      fillIndexOperations(remoteIndexEntries);

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {

        if (entry.getValue().type == ORecordOperation.UPDATED) {
          // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING HOOKS (LIKE UNDEXES)

          final ORecordInternal<?> record = entry.getValue().record.getRecord();
          final ORecordInternal<?> loadedRecord = record.getIdentity().copy().getRecord();
          if (loadedRecord == null)
            throw new ORecordNotFoundException(record.getIdentity().toString());

          if (loadedRecord.getRecordType() == ODocument.RECORD_TYPE && loadedRecord.getRecordType() == record.getRecordType()) {
            ((ODocument) loadedRecord).merge((ODocument) record, false, false);
            loadedRecord.getRecordVersion().copyFrom(record.getRecordVersion());
            entry.getValue().record = loadedRecord;
            
            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
            updatedRecords.put((ORecordId)entry.getKey(), entry.getValue().getRecord());
            
          }
        }

        addRecord(entry.getValue().getRecord(), entry.getValue().type, null);
      }
      tempEntries.clear();

      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord<?> record : createdRecords.values())
        unmarshallRecord(record);
      for (ORecordInternal<?> record : updatedRecords.values())
        unmarshallRecord(record);

    } catch (IOException e) {
      rollback();
      throw new OSerializationException("Cannot read transaction record from the network. Transaction aborted", e);
    }
  }

  @Override
  public ORecordInternal<?> getRecord(final ORID rid) {
    ORecordInternal<?> record = super.getRecord(rid);
    if (record == OTransactionRealAbstract.DELETED_RECORD)
      return record;
    else if (record == null && rid.isNew())
      // SEARCH BETWEEN CREATED RECORDS
      record = (ORecordInternal<?>) createdRecords.get(rid);

    return record;
  }

  private void fillIndexOperations(final ODocument remoteIndexEntries) {
    for (Entry<String, Object> indexEntry : remoteIndexEntries) {
      final String indexName = indexEntry.getKey();
      final ODocument indexDoc = (ODocument) indexEntry.getValue();
      if (indexDoc == null)
        continue;

      OTransactionIndexChanges transactionIndexChanges = indexEntries.get(indexEntry.getKey());

      if (transactionIndexChanges == null) {
        transactionIndexChanges = new OTransactionIndexChanges();
        indexEntries.put(indexEntry.getKey(), transactionIndexChanges);
      }

      final Boolean clearAll = (Boolean) indexDoc.field("clear");
      if (clearAll != null && clearAll)
        transactionIndexChanges.setCleared();

      final Collection<ODocument> entries = indexDoc.field("entries");
      if (entries == null)
        continue;

      for (final ODocument entry : entries) {
        final List<ODocument> operations = (List<ODocument>) entry.field("ops");
        if (operations == null)
          continue;

        final Object key;

        final String serializedKey = OStringSerializerHelper.decode((String) entry.field("k"));
        try {
          if (serializedKey.equals("*"))
            key = null;
          else {
            final ODocument keyContainer = new ODocument();
            keyContainer.setLazyLoad(false);

            keyContainer.fromString(serializedKey);

            final Object storedKey = keyContainer.field("key");
            if (storedKey instanceof List)
              key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
            else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
              key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
            } else
              key = storedKey;
          }
        } catch (IOException ioe) {
          throw new OTransactionException("Error during index changes deserialization. ", ioe);
        }

        for (final ODocument op : operations) {
          final int operation = (Integer) op.rawField("o");
          final OTransactionIndexChanges.OPERATION indexOperation = OTransactionIndexChanges.OPERATION.values()[operation];
          final OIdentifiable value = op.field("v", OType.LINK);

          if (key != null)
            transactionIndexChanges.getChangesPerKey(key).add(value, indexOperation);
          else
            transactionIndexChanges.getChangesCrossKey().add(value, indexOperation);

          if (value == null)
            continue;

          final ORID rid = value.getIdentity();
          List<OTransactionRecordIndexOperation> txIndexOperations = recordIndexOperations.get(rid);
          if (txIndexOperations == null) {
            txIndexOperations = new ArrayList<OTransactionRecordIndexOperation>();
            recordIndexOperations.put(rid, txIndexOperations);
          }

          txIndexOperations.add(new OTransactionRecordIndexOperation(indexName, key, indexOperation));
        }
      }
    }
  }

  public Map<ORecordId, ORecordInternal<?>> getCreatedRecords() {
    return createdRecords;
  }

  public Map<ORecordId, ORecordInternal<?>> getUpdatedRecords() {
    return updatedRecords;
  }

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  private void unmarshallRecord(final ORecord<?> iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();

      for (Entry<String, Object> field : ((ODocument) iRecord)) {
        if (field.getValue() instanceof ORecordLazyList)
          ((ORecordLazyList) field.getValue()).lazyLoad(true);
      }
    }
  }
}
