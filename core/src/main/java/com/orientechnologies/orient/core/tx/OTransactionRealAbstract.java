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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public abstract class OTransactionRealAbstract extends OTransactionAbstract {
  /**
   * USE THIS AS RESPONSE TO REPORT A DELETED RECORD IN TX
   */
  public static final ORecordFlat                             DELETED_RECORD        = new ORecordFlat();
  protected Map<ORID, ORecord>                                temp2persistent       = new HashMap<ORID, ORecord>();
  protected Map<ORID, ORecordOperation>                       allEntries            = new HashMap<ORID, ORecordOperation>();
  protected Map<ORID, ORecordOperation>                       recordEntries         = new LinkedHashMap<ORID, ORecordOperation>();
  protected Map<String, OTransactionIndexChanges>             indexEntries          = new LinkedHashMap<String, OTransactionIndexChanges>();
  protected Map<ORID, List<OTransactionRecordIndexOperation>> recordIndexOperations = new HashMap<ORID, List<OTransactionRecordIndexOperation>>();
  protected int                                               id;
  protected int                                               newObjectCounter      = -2;

  /**
   * This set is used to track which documents are changed during tx, if documents are changed but not saved all changes are made
   * during tx will be undone.
   */
  protected final Set<ODocument>                              changedDocuments      = new HashSet<ODocument>();

  /**
   * Represents information for each index operation for each record in DB.
   */
  public static final class OTransactionRecordIndexOperation {
    public String    index;
    public Object    key;
    public OPERATION operation;

    public OTransactionRecordIndexOperation(String index, Object key, OPERATION operation) {
      this.index = index;
      this.key = key;
      this.operation = operation;
    }
  }

  protected OTransactionRealAbstract(ODatabaseDocumentTx database, int id) {
    super(database);
    this.id = id;
  }

  @Override
  public boolean hasRecordCreation() {
    for (ORecordOperation op : recordEntries.values()) {
      if (op.type == ORecordOperation.CREATED)
        return true;
    }
    return false;
  }

  public void addChangedDocument(ODocument document) {
    if (getRecord(document.getIdentity()) == null) {
      changedDocuments.add(document);
    }
  }

  public void close() {
    super.close();

    for (final ORecordOperation recordOperation : getAllRecordEntries()) {
      final ORecord record = recordOperation.getRecord();
      if (record instanceof ODocument) {
        final ODocument document = (ODocument) record;

        if (document.isDirty()) {
          document.undo();
        }

        changedDocuments.remove(document);
      }
    }

    for (ODocument changedDocument : changedDocuments) {
      changedDocument.undo();
    }

    changedDocuments.clear();
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

  public void restore() {
    recordEntries.putAll(allEntries);
    allEntries.clear();
  }

  @Override
  public int getEntryCount() {
    return recordEntries.size();
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
      final ORecord record = temp2persistent.get(rid);
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

  public ORecord getRecord(final ORID rid) {
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
  public List<ORecordOperation> getNewRecordEntriesByClass(final OClass iClass, final boolean iPolymorphic) {
    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    if (iClass == null)
      // RETURN ALL THE RECORDS
      for (ORecordOperation entry : recordEntries.values()) {
        if (entry.type == ORecordOperation.CREATED)
          result.add(entry);
      }
    else {
      // FILTER RECORDS BY CLASSNAME
      for (ORecordOperation entry : recordEntries.values()) {
        if (entry.type == ORecordOperation.CREATED)
          if (entry.getRecord() != null && entry.getRecord() instanceof ODocument) {
            if (iPolymorphic) {
              if (iClass.isSuperClassOf(((ODocument) entry.getRecord()).getSchemaClass()))
                result.add(entry);
            } else if (iClass.getName().equals(((ODocument) entry.getRecord()).getClassName()))
              result.add(entry);
          }
      }
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

    final ODocument result = new ODocument().setAllowChainedAccess(false).setTrackingChanges(false);

    for (Entry<String, OTransactionIndexChanges> indexEntry : indexEntries.entrySet()) {
      final ODocument indexDoc = new ODocument().setTrackingChanges(false);
      ODocumentInternal.addOwner(indexDoc, result);

      result.field(indexEntry.getKey(), indexDoc, OType.EMBEDDED);

      if (indexEntry.getValue().cleared)
        indexDoc.field("clear", Boolean.TRUE);

      final List<ODocument> entries = new ArrayList<ODocument>();
      indexDoc.field("entries", entries, OType.EMBEDDEDLIST);

      // STORE INDEX ENTRIES
      for (OTransactionIndexChangesPerKey entry : indexEntry.getValue().changesPerKey.values())
        entries.add(serializeIndexChangeEntry(entry, indexDoc));

      indexDoc.field("nullEntries", serializeIndexChangeEntry(indexEntry.getValue().nullKeyChanges, indexDoc));
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
      final Object key, final OIdentifiable iValue) {
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
      }

      OTransactionIndexChangesPerKey changes = indexEntry.getChangesPerKey(key);

      changes.add(iValue, iOperation);

      if (iValue == null)
        return;

      List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(iValue.getIdentity());

      if (transactionIndexOperations == null) {
        transactionIndexOperations = new ArrayList<OTransactionRecordIndexOperation>();
        recordIndexOperations.put(iValue.getIdentity().copy(), transactionIndexOperations);
      }

      transactionIndexOperations.add(new OTransactionRecordIndexOperation(iIndexName, key, iOperation));
    }
  }

  public void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid) {
    if (oldRid.equals(newRid))
      // NO CHANGE, IGNORE IT
      return;

    final ORecordOperation rec = getRecordEntry(oldRid);
    if (rec != null) {
      if (allEntries.remove(oldRid) != null)
        allEntries.put(newRid, rec);

      if (recordEntries.remove(oldRid) != null)
        recordEntries.put(newRid, rec);

      if (!rec.getRecord().getIdentity().equals(newRid)) {
        ORecordInternal.onBeforeIdentityChanged(rec.getRecord());

        final ORecordId recordId = (ORecordId) rec.getRecord().getIdentity();
        if (recordId == null) {
          ORecordInternal.setIdentity(rec.getRecord(), new ORecordId(newRid));
        } else {
          recordId.clusterPosition = newRid.getClusterPosition();
          recordId.clusterId = newRid.getClusterId();
        }

        ORecordInternal.onAfterIdentityChanged(rec.getRecord());
      }
    }

    // UPDATE INDEXES
    final List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(oldRid);
    if (transactionIndexOperations != null) {
      for (final OTransactionRecordIndexOperation indexOperation : transactionIndexOperations) {
        OTransactionIndexChanges indexEntryChanges = indexEntries.get(indexOperation.index);
        if (indexEntryChanges == null)
          continue;

        final OTransactionIndexChangesPerKey changesPerKey = indexEntryChanges.getChangesPerKey(indexOperation.key);
        updateChangesIdentity(oldRid, newRid, changesPerKey);
      }
    }
  }

  protected void checkTransaction() {
    if (status == TXSTATUS.INVALID)
      throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
  }

  protected ODocument serializeIndexChangeEntry(OTransactionIndexChangesPerKey entry, final ODocument indexDoc) {
    // SERIALIZE KEY

    ODocument keyContainer = new ODocument();
    keyContainer.setTrackingChanges(false);

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

      } else
        keyContainer = null;
    } catch (IOException ioe) {
      throw new OTransactionException("Error during index changes serialization. ", ioe);
    }

    final List<ODocument> operations = new ArrayList<ODocument>();

    // SERIALIZE VALUES
    if (entry.entries != null && !entry.entries.isEmpty()) {
      for (OTransactionIndexEntry e : entry.entries) {
        final ODocument changeDoc = new ODocument().setAllowChainedAccess(false);
        ODocumentInternal.addOwner((ODocument) changeDoc, indexDoc);

        // SERIALIZE OPERATION
        changeDoc.field("o", e.operation.ordinal());

        if (e.value instanceof ORecord && e.value.getIdentity().isNew()) {
          final ORecord saved = temp2persistent.get(e.value.getIdentity());
          if (saved != null)
            e.value = saved;
          else
            ((ORecord) e.value).save();
        }

        changeDoc.field("v", e.value != null ? e.value.getIdentity() : null);

        operations.add(changeDoc);
      }
    }
    ODocument res = new ODocument();
    res.setTrackingChanges(false);
    ODocumentInternal.addOwner(res, indexDoc);
    return res.setAllowChainedAccess(false).field("k", keyContainer, OType.EMBEDDED).field("ops", operations, OType.EMBEDDEDLIST);
  }

  private void updateChangesIdentity(ORID oldRid, ORID newRid, OTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null)
      return;

    for (final OTransactionIndexEntry indexEntry : changesPerKey.entries)
      if (indexEntry.value.getIdentity().equals(oldRid))
        indexEntry.value = newRid;
  }
}
