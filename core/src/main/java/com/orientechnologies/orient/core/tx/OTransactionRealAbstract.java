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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public abstract class OTransactionRealAbstract extends OTransactionAbstract
    implements OTransactionInternal {
  protected Map<ORID, ORID> updatedRids = new HashMap<ORID, ORID>();
  protected Map<ORID, ORecordOperation> allEntries = new LinkedHashMap<ORID, ORecordOperation>();
  protected Map<String, OTransactionIndexChanges> indexEntries =
      new LinkedHashMap<String, OTransactionIndexChanges>();
  protected Map<ORID, List<OTransactionRecordIndexOperation>> recordIndexOperations =
      new HashMap<ORID, List<OTransactionRecordIndexOperation>>();
  protected int id;
  protected int newObjectCounter = -2;
  protected Map<String, Object> userData = new HashMap<String, Object>();
  private Map<ORID, LockedRecordMetadata> noTxLocks;
  private Optional<OTxMetadataHolder> metadata = Optional.empty();
  /**
   * token This set is used to track which documents are changed during tx, if documents are changed
   * but not saved all changes are made during tx will be undone.
   */
  protected final Set<ODocument> changedDocuments = new HashSet<ODocument>();

  private Optional<List<byte[]>> serializedOperations = Optional.empty();

  protected OTransactionRealAbstract(final ODatabaseDocumentInternal database, final int id) {
    super(database);
    this.id = id;
  }

  @Override
  public void addChangedDocument(ODocument document) {
    if (getRecord(document.getIdentity()) == null) {
      changedDocuments.add(document);
    }
  }

  public void close() {
    super.close();
    for (final ORecordOperation recordOperation : getRecordOperations()) {
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
      if (!changedDocument.isEmbedded()) {
        changedDocument.undo();
      }
    }
    changedDocuments.clear();
    updatedRids.clear();
    allEntries.clear();
    indexEntries.clear();
    recordIndexOperations.clear();
    newObjectCounter = -2;
    status = TXSTATUS.INVALID;

    database.setDefaultTransactionMode(getNoTxLocks());
    userData.clear();
  }

  public int getId() {
    return id;
  }

  public void clearRecordEntries() {}

  public void restore() {}

  @Override
  public int getEntryCount() {
    return allEntries.size();
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return allEntries.values();
  }

  public Collection<ORecordOperation> getRecordOperations() {
    return allEntries.values();
  }

  public ORecordOperation getRecordEntry(ORID ridPar) {
    ORID rid = ridPar;
    ORecordOperation entry;
    do {
      entry = allEntries.get(rid);
      if (entry == null) {
        rid = updatedRids.get(rid);
      }
    } while (entry == null && rid != null && !rid.equals(ridPar));
    return entry;
  }

  public ORecord getRecord(final ORID rid) {
    final ORecordOperation e = getRecordEntry(rid);
    if (e != null)
      if (e.type == ORecordOperation.DELETED) return OTransactionAbstract.DELETED_RECORD;
      else return e.getRecord();
    return null;
  }

  /** Called by class iterator. */
  public List<ORecordOperation> getNewRecordEntriesByClass(
      final OClass iClass, final boolean iPolymorphic) {
    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    if (iClass == null)
      // RETURN ALL THE RECORDS
      for (ORecordOperation entry : allEntries.values()) {
        if (entry.type == ORecordOperation.CREATED) result.add(entry);
      }
    else {
      // FILTER RECORDS BY CLASSNAME
      for (ORecordOperation entry : allEntries.values()) {
        if (entry.type == ORecordOperation.CREATED)
          if (entry.getRecord() != null && entry.getRecord() instanceof ODocument) {
            if (iPolymorphic) {
              if (iClass.isSuperClassOf(
                  ODocumentInternal.getImmutableSchemaClass(((ODocument) entry.getRecord()))))
                result.add(entry);
            } else if (iClass.getName().equals(((ODocument) entry.getRecord()).getClassName()))
              result.add(entry);
          }
      }
    }

    return result;
  }

  /** Called by cluster iterator. */
  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    final List<ORecordOperation> result = new ArrayList<ORecordOperation>();

    if (iIds == null)
      // RETURN ALL THE RECORDS
      for (ORecordOperation entry : allEntries.values()) {
        if (entry.type == ORecordOperation.CREATED) result.add(entry);
      }
    else
      // FILTER RECORDS BY ID
      for (ORecordOperation entry : allEntries.values()) {
        for (int id : iIds) {
          if (entry.getRecord() != null
              && entry.getRecord().getIdentity().getClusterId() == id
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
      if (list == null) list = new ArrayList<String>();
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

      if (indexEntry.getValue().cleared) indexDoc.field("clear", Boolean.TRUE);

      final List<ODocument> entries = new ArrayList<ODocument>();
      indexDoc.field("entries", entries, OType.EMBEDDEDLIST);

      // STORE INDEX ENTRIES
      for (OTransactionIndexChangesPerKey entry : indexEntry.getValue().changesPerKey.values()) {
        if (!entry.clientTrackOnly) entries.add(serializeIndexChangeEntry(entry, indexDoc));
      }

      indexDoc.field(
          "nullEntries", serializeIndexChangeEntry(indexEntry.getValue().nullKeyChanges, indexDoc));
    }

    indexEntries.clear();

    return result;
  }

  public Map<String, OTransactionIndexChanges> getIndexOperations() {
    return indexEntries;
  }

  /**
   * Bufferizes index changes to be flushed at commit time.
   *
   * @return
   */
  public OTransactionIndexChanges getIndexChanges(final String iIndexName) {
    return indexEntries.get(iIndexName);
  }

  public OTransactionIndexChanges getIndexChangesInternal(final String indexName) {
    if (getDatabase().isRemote()) return null;
    return getIndexChanges(indexName);
  }

  public void addIndexEntry(
      final OIndex delegate,
      final String iIndexName,
      final OTransactionIndexChanges.OPERATION iOperation,
      final Object key,
      final OIdentifiable iValue) {
    addIndexEntry(delegate, iIndexName, iOperation, key, iValue, false);
  }

  /** Bufferizes index changes to be flushed at commit time. */
  public void addIndexEntry(
      final OIndex delegate,
      final String iIndexName,
      final OTransactionIndexChanges.OPERATION iOperation,
      final Object key,
      final OIdentifiable iValue,
      boolean clientTrackOnly) {
    OTransactionIndexChanges indexEntry = indexEntries.get(iIndexName);
    if (indexEntry == null) {
      indexEntry = new OTransactionIndexChanges();
      indexEntries.put(iIndexName, indexEntry);
    }

    if (iOperation == OPERATION.CLEAR) indexEntry.setCleared();
    else {
      OTransactionIndexChangesPerKey changes = indexEntry.getChangesPerKey(key);
      changes.clientTrackOnly = clientTrackOnly;
      changes.add(iValue, iOperation);

      if (iValue == null) return;

      List<OTransactionRecordIndexOperation> transactionIndexOperations =
          recordIndexOperations.get(iValue.getIdentity());

      if (transactionIndexOperations == null) {
        transactionIndexOperations = new ArrayList<OTransactionRecordIndexOperation>();
        recordIndexOperations.put(iValue.getIdentity().copy(), transactionIndexOperations);
      }

      transactionIndexOperations.add(
          new OTransactionRecordIndexOperation(iIndexName, key, iOperation));
    }
  }

  public void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid) {
    if (oldRid.equals(newRid))
      // NO CHANGE, IGNORE IT
      return;

    // XXX: Identity update may mutate the index keys, so we have to identify and reinsert
    // potentially affected index keys to keep
    // the OTransactionIndexChanges.changesPerKey in a consistent state.

    final List<KeyChangesUpdateRecord> keyRecordsToReinsert =
        new ArrayList<KeyChangesUpdateRecord>();
    final ODatabaseDocumentInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    for (Entry<String, OTransactionIndexChanges> entry : indexEntries.entrySet()) {
      final OIndex index = indexManager.getIndex(database, entry.getKey());
      if (index == null)
        throw new OTransactionException(
            "Cannot find index '" + entry.getValue() + "' while committing transaction");

      final Dependency[] fieldRidDependencies = getIndexFieldRidDependencies(index);
      if (!isIndexMayDependOnRids(fieldRidDependencies)) continue;

      final OTransactionIndexChanges indexChanges = entry.getValue();
      for (final Iterator<OTransactionIndexChangesPerKey> iterator =
              indexChanges.changesPerKey.values().iterator();
          iterator.hasNext(); ) {
        final OTransactionIndexChangesPerKey keyChanges = iterator.next();
        if (isIndexKeyMayDependOnRid(keyChanges.key, oldRid, fieldRidDependencies)) {
          keyRecordsToReinsert.add(new KeyChangesUpdateRecord(keyChanges, indexChanges));
          iterator.remove();
        }
      }
    }

    // Update the identity.

    final ORecordOperation rec = getRecordEntry(oldRid);
    if (rec != null) {
      updatedRids.put(newRid.copy(), oldRid.copy());

      if (!rec.getRecord().getIdentity().equals(newRid)) {
        ORecordInternal.onBeforeIdentityChanged(rec.getRecord());

        final ORecordId recordId = (ORecordId) rec.getRecord().getIdentity();
        if (recordId == null) {
          ORecordInternal.setIdentity(rec.getRecord(), new ORecordId(newRid));
        } else {
          recordId.setClusterPosition(newRid.getClusterPosition());
          recordId.setClusterId(newRid.getClusterId());
        }

        ORecordInternal.onAfterIdentityChanged(rec.getRecord());
      }
    }

    // Reinsert the potentially affected index keys.

    for (KeyChangesUpdateRecord record : keyRecordsToReinsert)
      record.indexChanges.changesPerKey.put(record.keyChanges.key, record.keyChanges);

    // Update the indexes.

    ORecordOperation val = getRecordEntry(oldRid);
    final List<OTransactionRecordIndexOperation> transactionIndexOperations =
        recordIndexOperations.get(val != null ? val.getRID() : null);
    if (transactionIndexOperations != null) {
      for (final OTransactionRecordIndexOperation indexOperation : transactionIndexOperations) {
        OTransactionIndexChanges indexEntryChanges = indexEntries.get(indexOperation.index);
        if (indexEntryChanges == null) continue;
        final OTransactionIndexChangesPerKey keyChanges;
        if (indexOperation.key == null) {
          keyChanges = indexEntryChanges.nullKeyChanges;
        } else {
          keyChanges = indexEntryChanges.changesPerKey.get(indexOperation.key);
        }
        if (keyChanges != null) updateChangesIdentity(oldRid, newRid, keyChanges);
      }
    }
  }

  protected void checkTransactionValid() {
    if (status == TXSTATUS.INVALID) {
      throw new OTransactionException(
          "Invalid state of the transaction. The transaction must be begun.");
    }
  }

  protected ODocument serializeIndexChangeEntry(
      OTransactionIndexChangesPerKey entry, final ODocument indexDoc) {
    // SERIALIZE KEY

    ODocument keyContainer = new ODocument();
    keyContainer.setTrackingChanges(false);

    if (entry.key != null) {
      if (entry.key instanceof OCompositeKey) {
        final List<Object> keys = ((OCompositeKey) entry.key).getKeys();

        keyContainer.field("key", keys, OType.EMBEDDEDLIST);
        keyContainer.field("binary", false);
      } else {
        keyContainer.field("key", entry.key);
        keyContainer.field("binary", false);
      }

    } else keyContainer = null;

    final List<ODocument> operations = new ArrayList<ODocument>();

    // SERIALIZE VALUES
    if (!entry.isEmpty()) {
      for (OTransactionIndexEntry e : entry.getEntriesAsList()) {

        final ODocument changeDoc = new ODocument().setAllowChainedAccess(false);
        ODocumentInternal.addOwner(changeDoc, indexDoc);

        // SERIALIZE OPERATION
        changeDoc.field("o", e.getOperation().ordinal());

        if (e.getValue() instanceof ORecord && e.getValue().getIdentity().isNew()) {
          final ORecord saved = getRecord(e.getValue().getIdentity());
          if (saved != null) e.setValue(saved);
          else ((ORecord) e.getValue()).save();
        }

        changeDoc.field("v", e.getValue() != null ? e.getValue().getIdentity() : null);

        operations.add(changeDoc);
      }
    }
    ODocument res = new ODocument();
    res.setTrackingChanges(false);
    ODocumentInternal.addOwner(res, indexDoc);
    return res.setAllowChainedAccess(false)
        .field("k", keyContainer, OType.EMBEDDED)
        .field("ops", operations, OType.EMBEDDEDLIST);
  }

  private void updateChangesIdentity(
      ORID oldRid, ORID newRid, OTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null) return;

    for (final OTransactionIndexEntry indexEntry : changesPerKey.getEntriesAsList())
      if (indexEntry.getValue().getIdentity().equals(oldRid)) indexEntry.setValue(newRid);
  }

  @Override
  public void setCustomData(String iName, Object iValue) {
    userData.put(iName, iValue);
  }

  @Override
  public Object getCustomData(String iName) {
    return userData.get(iName);
  }

  private static Dependency[] getIndexFieldRidDependencies(OIndex index) {
    final OIndexDefinition definition = index.getDefinition();

    if (definition == null) // type for untyped index is still not resolved
    return null;

    final OType[] types = definition.getTypes();
    final Dependency[] dependencies = new Dependency[types.length];

    for (int i = 0; i < types.length; ++i) dependencies[i] = getTypeRidDependency(types[i]);

    return dependencies;
  }

  private static boolean isIndexMayDependOnRids(Dependency[] fieldDependencies) {
    if (fieldDependencies == null) return true;

    for (Dependency dependency : fieldDependencies)
      switch (dependency) {
        case Unknown:
        case Yes:
          return true;
        case No:
          break; // do nothing
      }

    return false;
  }

  private static boolean isIndexKeyMayDependOnRid(
      Object key, ORID rid, Dependency[] keyDependencies) {
    if (key instanceof OCompositeKey) {
      final List<Object> subKeys = ((OCompositeKey) key).getKeys();
      for (int i = 0; i < subKeys.size(); ++i)
        if (isIndexKeyMayDependOnRid(
            subKeys.get(i), rid, keyDependencies == null ? null : keyDependencies[i])) return true;
      return false;
    }

    return isIndexKeyMayDependOnRid(key, rid, keyDependencies == null ? null : keyDependencies[0]);
  }

  private static boolean isIndexKeyMayDependOnRid(Object key, ORID rid, Dependency dependency) {
    if (dependency == Dependency.No) return false;

    if (key instanceof OIdentifiable) return key.equals(rid);

    return dependency == Dependency.Unknown || dependency == null;
  }

  private static Dependency getTypeRidDependency(OType type) {
    switch (type) {
      case CUSTOM:
      case ANY:
        return Dependency.Unknown;

      case EMBEDDED:
      case LINK:
        return Dependency.Yes;

      case LINKLIST:
      case LINKSET:
      case LINKMAP:
      case LINKBAG:
      case EMBEDDEDLIST:
      case EMBEDDEDSET:
      case EMBEDDEDMAP:
        assert false; // under normal conditions, collection field type is already resolved to its
        // component type
        return Dependency.Unknown; // fallback to the safest variant, just in case

      default: // all other primitive types which doesn't depend on rids
        return Dependency.No;
    }
  }

  private enum Dependency {
    Unknown,
    Yes,
    No
  }

  private static class KeyChangesUpdateRecord {
    public final OTransactionIndexChangesPerKey keyChanges;
    public final OTransactionIndexChanges indexChanges;

    public KeyChangesUpdateRecord(
        OTransactionIndexChangesPerKey keyChanges, OTransactionIndexChanges indexChanges) {
      this.keyChanges = keyChanges;
      this.indexChanges = indexChanges;
    }
  }

  public Map<ORID, ORID> getUpdatedRids() {
    return updatedRids;
  }

  public int getNewObjectCounter() {
    return newObjectCounter;
  }

  public void setNoTxLocks(Map<ORID, LockedRecordMetadata> noTxLocks) {
    this.noTxLocks = noTxLocks;
  }

  public Map<ORID, LockedRecordMetadata> getNoTxLocks() {
    return noTxLocks;
  }

  @Override
  public Optional<byte[]> getMetadata() {
    return metadata.map((h) -> h.metadata());
  }

  @Override
  public void storageBegun() {
    if (metadata.isPresent()) {
      metadata.get().notifyMetadataRead();
    }
  }

  @Override
  public void setMetadataHolder(Optional<OTxMetadataHolder> metadata) {
    this.metadata = metadata;
  }

  @Override
  public void prepareSerializedOperations() throws IOException {
    List<byte[]> operations = new ArrayList<>();
    for (ORecordOperation value : allEntries.values()) {
      OTransactionDataChange change = new OTransactionDataChange(value);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      change.serialize(new DataOutputStream(out));
      operations.add(out.toByteArray());
    }
    this.serializedOperations = Optional.of(operations);
  }

  public Iterator<byte[]> getSerializedOperations() {
    if (serializedOperations.isPresent()) {
      return serializedOperations.get().iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public void resetAllocatedIds() {
    for (Map.Entry<ORID, ORecordOperation> op : allEntries.entrySet()) {
      if (op.getValue().type == ORecordOperation.CREATED) {
        ORecordId oldNew =
            new ORecordId(op.getKey().getClusterId(), op.getKey().getClusterPosition());
        updateIdentityAfterCommit(op.getValue().getRID(), oldNew);
        updatedRids.remove(op.getValue().getRID());
      }
    }
  }
}
