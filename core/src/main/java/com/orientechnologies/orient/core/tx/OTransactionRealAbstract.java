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
package com.orientechnologies.orient.core.tx;

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
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

public abstract class OTransactionRealAbstract extends OTransactionAbstract {
  protected Map<ORID, ORecord<?>>                             temp2persistent       = new HashMap<ORID, ORecord<?>>();
  protected Map<ORID, ORecordOperation>                       allEntries            = new HashMap<ORID, ORecordOperation>();
  protected Map<ORID, ORecordOperation>                       recordEntries         = new LinkedHashMap<ORID, ORecordOperation>();
  protected Map<String, OTransactionIndexChanges>             indexEntries          = new LinkedHashMap<String, OTransactionIndexChanges>();
  protected Map<ORID, List<OTransactionRecordIndexOperation>> recordIndexOperations = new HashMap<ORID, List<OTransactionRecordIndexOperation>>();
  protected int                                               id;
  private final OOperationUnitId                              operationUnitId;

  protected int                                               newObjectCounter      = -2;

  /**
   * USE THIS AS RESPONSE TO REPORT A DELETED RECORD IN TX
   */
  public static final ORecordFlat                             DELETED_RECORD        = new ORecordFlat();

  /**
   * Represents information for each index operation for each record in DB.
   */
  public static final class OTransactionRecordIndexOperation {
    public OTransactionRecordIndexOperation(String index, Object key, OPERATION operation) {
      this.index = index;
      this.key = key;
      this.operation = operation;
    }

    public String    index;
    public Object    key;
    public OPERATION operation;
  }

  protected OTransactionRealAbstract(ODatabaseRecordTx database, int id) {
    super(database);
    this.id = id;
    this.operationUnitId = OOperationUnitId.generateId();
  }

  public void close() {
    temp2persistent.clear();
    allEntries.clear();
    recordEntries.clear();
    indexEntries.clear();
    recordIndexOperations.clear();
    newObjectCounter = -2;
    status = TXSTATUS.INVALID;

    database.setDefaultTransactionMode();
  }

  public int getId() {
    return id;
  }

  public void clearRecordEntries() {
    for (Entry<ORID, ORecordOperation> entry : recordEntries.entrySet()) {
      final ORID key = entry.getKey();

      // ID NEW CREATE A COPY OF RID TO AVOID IT CHANGES IDENTITY+HASHCODE AND IT'S UNREACHEABLE THEREAFTER
      allEntries.put(key.isNew() ? key.copy() : key, entry.getValue());
    }

    recordEntries.clear();
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return recordEntries.values();
  }

  public Collection<ORecordOperation> getAllRecordEntries() {
    return allEntries.values();
  }

  public ORecordOperation getRecordEntry(ORID rid) {
    ORecordOperation e = allEntries.get(rid);
    if (e != null)
      return e;

    if (rid.isTemporary()) {
      final ORecord<?> record = temp2persistent.get(rid);
      if (record != null && !record.getIdentity().equals(rid))
        rid = record.getIdentity();
    }

    e = recordEntries.get(rid);
    if (e != null)
      return e;

    e = allEntries.get(rid);
    if (e != null)
      return e;

    return null;
  }

  public ORecordInternal<?> getRecord(final ORID rid) {
    final ORecordOperation e = getRecordEntry(rid);
    if (e != null)
      if (e.type == ORecordOperation.DELETED)
        return DELETED_RECORD;
      else
        return e.getRecord();
    return null;
  }

  /**
   * Called by class iterator.
   */
  public List<ORecordOperation> getRecordEntriesByClass(final String iClassName) {
    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    if (iClassName == null || iClassName.length() == 0)
      // RETURN ALL THE RECORDS
      for (ORecordOperation entry : recordEntries.values()) {
        result.add(entry);
      }
    else
      // FILTER RECORDS BY CLASSNAME
      for (ORecordOperation entry : recordEntries.values()) {
        if (entry.getRecord() != null && entry.getRecord() instanceof ODocument
            && iClassName.equals(((ODocument) entry.getRecord()).getClassName()))
          result.add(entry);
      }

    return result;
  }

  /**
   * Called by cluster iterator.
   */
  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    if (iIds == null)
      // RETURN ALL THE RECORDS
      for (ORecordOperation entry : recordEntries.values()) {
        if (entry.type == ORecordOperation.CREATED)
          result.add(entry);
      }
    else
      // FILTER RECORDS BY ID
      for (ORecordOperation entry : recordEntries.values()) {
        for (int id : iIds) {
          if (entry.getRecord() != null && entry.getRecord().getIdentity().getClusterId() == id
              && entry.type == ORecordOperation.CREATED) {
            result.add(entry);
            break;
          }
        }
      }

    return result;
  }

  public void clearIndexEntries() {
    indexEntries.clear();
    recordIndexOperations.clear();
  }

  public List<String> getInvolvedIndexes() {
    List<String> list = null;
    for (String indexName : indexEntries.keySet()) {
      if (list == null)
        list = new ArrayList<String>();
      list.add(indexName);
    }
    return list;
  }

  public ODocument getIndexChanges() {

    final ODocument result = new ODocument().setAllowChainedAccess(false);

    for (Entry<String, OTransactionIndexChanges> indexEntry : indexEntries.entrySet()) {
      final ODocument indexDoc = new ODocument().addOwner(result);

      result.field(indexEntry.getKey(), indexDoc, OType.EMBEDDED);

      if (indexEntry.getValue().cleared)
        indexDoc.field("clear", Boolean.TRUE);

      final List<ODocument> entries = new ArrayList<ODocument>();
      indexDoc.field("entries", entries, OType.EMBEDDEDLIST);

      if (indexEntry.getValue().changesCrossKey != null)
        serializeIndexChangeEntry(indexEntry.getValue().changesCrossKey, indexDoc, entries);

      // STORE INDEX ENTRIES
      for (OTransactionIndexChangesPerKey entry : indexEntry.getValue().changesPerKey.values())
        serializeIndexChangeEntry(entry, indexDoc, entries);
    }

    indexEntries.clear();

    return result;
  }

  /**
   * Bufferizes index changes to be flushed at commit time.
   * 
   * @return
   */
  public OTransactionIndexChanges getIndexChanges(final String iIndexName) {
    return indexEntries.get(iIndexName);
  }

  /**
   * Bufferizes index changes to be flushed at commit time.
   */
  public void addIndexEntry(final OIndex<?> delegate, final String iIndexName, final OTransactionIndexChanges.OPERATION iOperation,
      final Object iKey, final OIdentifiable iValue) {
    OTransactionIndexChanges indexEntry = indexEntries.get(iIndexName);
    if (indexEntry == null) {
      indexEntry = new OTransactionIndexChanges();
      indexEntries.put(iIndexName, indexEntry);
    }

    if (iOperation == OPERATION.CLEAR)
      indexEntry.setCleared();
    else {
      if (iOperation == OPERATION.REMOVE && iValue != null && iValue.getIdentity().isTemporary()) {

        // TEMPORARY RECORD: JUST REMOVE IT
        for (OTransactionIndexChangesPerKey changes : indexEntry.changesPerKey.values())
          for (int i = 0; i < changes.entries.size(); ++i)
            if (changes.entries.get(i).value.equals(iValue)) {
              changes.entries.remove(i);
              break;
            }

        OTransactionIndexChangesPerKey changes = indexEntry.getChangesCrossKey();
        for (int i = 0; i < changes.entries.size(); ++i)
          if (changes.entries.get(i).value.equals(iValue)) {
            changes.entries.remove(i);
            break;
          }
      }

      OTransactionIndexChangesPerKey changes = iKey != null ? indexEntry.getChangesPerKey(iKey) : indexEntry.getChangesCrossKey();

      changes.add(iValue, iOperation);

      if (iValue == null)
        return;

      List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(iValue.getIdentity());

      if (transactionIndexOperations == null) {
        transactionIndexOperations = new ArrayList<OTransactionRecordIndexOperation>();
        recordIndexOperations.put(iValue.getIdentity().copy(), transactionIndexOperations);
      }

      transactionIndexOperations.add(new OTransactionRecordIndexOperation(iIndexName, iKey, iOperation));
    }
  }

  public void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid) {
    if (oldRid.equals(newRid))
      // NO CHANGE, IGNORE IT
      return;

    if (oldRid.isNew()) {
      // REMOVE AND RE-PUT THE OPERATION BECAUSE KEY IS CHANGED
      final ORecordOperation rec = allEntries.remove(oldRid);
      if (rec != null)
        allEntries.put(newRid, rec);
    }

    // UPDATE INDEXES
    final List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(oldRid);
    if (transactionIndexOperations != null) {
      for (final OTransactionRecordIndexOperation indexOperation : transactionIndexOperations) {
        OTransactionIndexChanges indexEntryChanges = indexEntries.get(indexOperation.index);
        if (indexEntryChanges == null)
          continue;

        final OTransactionIndexChangesPerKey changesPerKey;
        if (indexOperation.key != null)
          changesPerKey = indexEntryChanges.getChangesPerKey(indexOperation.key);
        else
          changesPerKey = indexEntryChanges.changesCrossKey;

        updateChangesIdentity(oldRid, newRid, changesPerKey);
      }
    }
  }

  private void updateChangesIdentity(ORID oldRid, ORID newRid, OTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null)
      return;

    for (final OTransactionIndexEntry indexEntry : changesPerKey.entries)
      if (indexEntry.value.getIdentity().equals(oldRid))
        indexEntry.value = newRid;
  }

  protected void checkTransaction() {
    if (status == TXSTATUS.INVALID)
      throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
  }

  protected void serializeIndexChangeEntry(OTransactionIndexChangesPerKey entry, final ODocument indexDoc,
      final List<ODocument> entries) {
    // SERIALIZE KEY

    final String key;
    final ODocument keyContainer = new ODocument();

    try {
      if (entry.key != null) {
        if (entry.key instanceof OCompositeKey) {
          final List<Object> keys = ((OCompositeKey) entry.key).getKeys();

          keyContainer.field("key", keys, OType.EMBEDDEDLIST);
          keyContainer.field("binary", false);
        } else if (!(entry.key instanceof ORecordElement) && (entry.key instanceof OSerializableStream)) {
          keyContainer.field("key", OStreamSerializerAnyStreamable.INSTANCE.toStream(entry.key), OType.BINARY);
          keyContainer.field("binary", true);
        } else {
          keyContainer.field("key", entry.key);
          keyContainer.field("binary", false);
        }

        key = ORecordSerializerSchemaAware2CSV.INSTANCE.toString(keyContainer, null, false).toString();
      } else
        key = "*";
    } catch (IOException ioe) {
      throw new OTransactionException("Error during index changes serialization. ", ioe);
    }

    final List<ODocument> operations = new ArrayList<ODocument>();

    // SERIALIZE VALUES
    if (entry.entries != null && !entry.entries.isEmpty()) {
      for (OTransactionIndexEntry e : entry.entries) {
        final ODocument changeDoc = new ODocument().addOwner(indexDoc).setAllowChainedAccess(false);

        // SERIALIZE OPERATION
        changeDoc.field("o", e.operation.ordinal());

        if (e.value instanceof ORecord<?> && e.value.getIdentity().isNew()) {
          final ORecord<?> saved = temp2persistent.get(e.value.getIdentity());
          if (saved != null)
            e.value = saved;
          else
            ((ORecord<?>) e.value).save();
        }

        changeDoc.field("v", e.value != null ? e.value.getIdentity() : null);

        operations.add(changeDoc);
      }
    }

    entries.add(new ODocument().addOwner(indexDoc).setAllowChainedAccess(false).field("k", OStringSerializerHelper.encode(key))
        .field("ops", operations, OType.EMBEDDEDLIST));
  }
}
