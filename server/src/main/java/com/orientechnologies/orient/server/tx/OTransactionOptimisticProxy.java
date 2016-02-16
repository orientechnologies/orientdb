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
package com.orientechnologies.orient.server.tx;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OSimpleVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

public class OTransactionOptimisticProxy extends OTransactionOptimistic {
  private final Map<ORID, ORecordOperation> tempEntries    = new LinkedHashMap<ORID, ORecordOperation>();
  private final Map<ORecordId, ORecord>     createdRecords = new HashMap<ORecordId, ORecord>();
  private final Map<ORecordId, ORecord>     updatedRecords = new HashMap<ORecordId, ORecord>();
  @Deprecated
  private final int                         clientTxId;
  private final OChannelBinary              channel;
  private final short                       protocolVersion;
  private ONetworkProtocolBinary            oNetworkProtocolBinary;

  public OTransactionOptimisticProxy(final ODatabaseDocumentTx iDatabase, final OChannelBinary iChannel, short protocolVersion,
      ONetworkProtocolBinary oNetworkProtocolBinary) throws IOException {
    super(iDatabase);
    channel = iChannel;
    clientTxId = iChannel.readInt();
    this.protocolVersion = protocolVersion;
    this.oNetworkProtocolBinary = oNetworkProtocolBinary;
  }

  @Override
  public void begin() {
    super.begin();
    // Needed for keep the exception and insure that all data is read from the socket.
    OException toThrow = null;

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
          oNetworkProtocolBinary.fillRecord(rid, channel.readBytes(), OVersionFactory.instance().createVersion(), entry.getRecord(),
              database);

          // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
          createdRecords.put(rid.copy(), entry.getRecord());
          break;

        case ORecordOperation.UPDATED:
          ORecordVersion version = channel.readVersion();
          byte[] bytes = channel.readBytes();
          oNetworkProtocolBinary.fillRecord(rid, bytes, version, entry.getRecord(), database);
          if (protocolVersion >= 23)
            ORecordInternal.setContentChanged(entry.getRecord(), channel.readBoolean());
          break;

        case ORecordOperation.DELETED:
          // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
          final ORecord rec = rid.getRecord();
          ORecordVersion deleteVersion = channel.readVersion();
          if (rec == null)
            toThrow = new OConcurrentModificationException(rid.getIdentity(), new OSimpleVersion(-1), deleteVersion,
                ORecordOperation.DELETED);

          ORecordInternal.setVersion(rec, deleteVersion.getCounter());
          entry.setRecord(rec);
          break;

        default:
          throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }

      if (toThrow != null)
        throw toThrow;

      if (lastTxStatus == -1)
        // ABORT TX
        throw new OTransactionAbortedException("Transaction aborted by the client");


      final ODocument remoteIndexEntries = new ODocument(channel.readBytes());
      fillIndexOperations(remoteIndexEntries);

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {

        if (entry.getValue().type == ORecordOperation.UPDATED) {
          // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING HOOKS (LIKE INDEXES)
          final ORecord record = entry.getValue().record.getRecord();
          final boolean contentChanged = ORecordInternal.isContentChanged(record);
          final ORecord loadedRecord = record.getIdentity().copy().getRecord();
          if (loadedRecord == null)
            throw new ORecordNotFoundException(record.getIdentity().toString());

          if (ORecordInternal.getRecordType(loadedRecord) == ODocument.RECORD_TYPE
              && ORecordInternal.getRecordType(loadedRecord) == ORecordInternal.getRecordType(record)) {
            ((ODocument) loadedRecord).merge((ODocument) record, false, false);
            ((ODocument) loadedRecord).setDirty();
            ORecordInternal.setContentChanged(loadedRecord, contentChanged);

            loadedRecord.getRecordVersion().copyFrom(record.getRecordVersion());
            entry.getValue().record = loadedRecord;

            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
            updatedRecords.put((ORecordId) entry.getKey(), entry.getValue().getRecord());

          }
        }

        addRecord(entry.getValue().getRecord(), entry.getValue().type, null);
      }
      tempEntries.clear();

      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord record : createdRecords.values())
        unmarshallRecord(record);
      for (ORecord record : updatedRecords.values())
        unmarshallRecord(record);

    } catch (IOException e) {
      rollback();
      throw new OSerializationException("Cannot read transaction record from the network. Transaction aborted", e);
    }
  }

  @Override
  public ORecord getRecord(final ORID rid) {
    ORecord record = super.getRecord(rid);
    if (record == OTransactionRealAbstract.DELETED_RECORD)
      return record;
    else if (record == null && rid.isNew())
      // SEARCH BETWEEN CREATED RECORDS
      record = createdRecords.get(rid);

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

      final Boolean clearAll = indexDoc.field("clear");
      if (clearAll != null && clearAll)
        transactionIndexChanges.setCleared();

      final Collection<ODocument> entries = indexDoc.field("entries");
      if (entries == null)
        continue;

      for (final ODocument entry : entries) {
        final List<ODocument> operations = entry.field("ops");
        if (operations == null)
          continue;

        final Object key;
        try {
          ODocument keyContainer;
          if (protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_24) {

            final String serializedKey = OStringSerializerHelper.decode((String) entry.field("k"));
            if (serializedKey.equals("*"))
              keyContainer = null;
            else {
              keyContainer = new ODocument();
              keyContainer.setLazyLoad(false);
              ORecordSerializerSchemaAware2CSV.INSTANCE.fromString(serializedKey, keyContainer, null);
            }
          } else {
            keyContainer = entry.field("k");
          }
          if (keyContainer != null) {
            final Object storedKey = keyContainer.field("key");
            if (storedKey instanceof List)
              key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
            else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
              key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
            } else
              key = storedKey;
          } else
            key = null;
        } catch (IOException ioe) {
          throw new OTransactionException("Error during index changes deserialization. ", ioe);
        }

        for (final ODocument op : operations) {
          final int operation = (Integer) op.rawField("o");
          final OTransactionIndexChanges.OPERATION indexOperation = OTransactionIndexChanges.OPERATION.values()[operation];
          final OIdentifiable value = op.field("v");

          transactionIndexChanges.getChangesPerKey(key).add(value, indexOperation);

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

  public Map<ORecordId, ORecord> getCreatedRecords() {
    return createdRecords;
  }

  public Map<ORecordId, ORecord> getUpdatedRecords() {
    return updatedRecords;
  }

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  private void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();

      for (Entry<String, Object> field : ((ODocument) iRecord)) {
        final Object value = field.getValue();
        if (value instanceof ORecordLazyList)
          ((ORecordLazyList) field.getValue()).lazyLoad(true);
        else if (value instanceof ORidBag)
          ((ORidBag) value).convertLinks2Records();
      }
    }
  }
}
