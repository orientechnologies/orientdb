package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.ArrayList;
import java.util.List;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  private List<ORecordOperation> changes;

  public OTransactionOptimisticDistributed(ODatabaseDocumentInternal database, List<ORecordOperation> changes) {
    super(database);
    this.changes = changes;
  }

  @Override
  public void begin() {
    super.begin();
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      resolveTracking(change);
    }

  }

  private void resolveTracking(ORecordOperation change) {
    List<OClassIndexManager.IndexChange> changes = new ArrayList<>();
    ODocument rec = (ODocument) change.getRecord();
    switch (change.getType()) {
    case ORecordOperation.CREATED: {
      ODocument doc = (ODocument) change.getRecord();
      OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, database);
      OLiveQueryHookV2.addOp(doc, ORecordOperation.CREATED, database);
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
      if (clazz != null) {
        OClassIndexManager.processIndexOnCreate(database, rec, changes);
        if (clazz.isFunction()) {
          database.getSharedContext().getFunctionLibrary().createdFunction(doc);
          database.getSharedContext().getOrientDB().getScriptManager().close(database.getName());
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate().onSequenceCreated(database, doc);
        }
        if (clazz.isScheduler()) {
          database.getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
        }
      }
    }
    break;
    case ORecordOperation.UPDATED: {
      OIdentifiable updateRecord = change.getRecord();
      if (updateRecord instanceof ODocument) {
        ODocument updateDoc = (ODocument) updateRecord;
        OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
        OLiveQueryHookV2.addOp(updateDoc, ORecordOperation.UPDATED, database);
        OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
        if (clazz != null) {
          OClassIndexManager.processIndexOnUpdate(database, updateDoc, changes);
          if (clazz.isFunction()) {
            database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
            database.getSharedContext().getOrientDB().getScriptManager().close(database.getName());
          }
          if (clazz.isSequence()) {
            ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate()
                .onSequenceUpdated(database, updateDoc);
          }
        }
      }
    }
    break;
    case ORecordOperation.DELETED: {
      ODocument doc = (ODocument) change.getRecord();
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
      if (clazz != null) {
        OClassIndexManager.processIndexOnDelete(database, rec, changes);
        if (clazz.isFunction()) {
          database.getSharedContext().getFunctionLibrary().droppedFunction(doc);
          database.getSharedContext().getOrientDB().getScriptManager().close(database.getName());
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate().onSequenceDropped(database, doc);
        }
        if (clazz.isScheduler()) {
          final String eventName = doc.field(OScheduledEvent.PROP_NAME);
          database.getSharedContext().getScheduler().removeEventInternal(eventName);
        }
      }
      OLiveQueryHook.addOp(doc, ORecordOperation.DELETED, database);
      OLiveQueryHookV2.addOp(doc, ORecordOperation.DELETED, database);
    }
    break;
    case ORecordOperation.LOADED:
      break;
    default:
      break;
    }

    for (OClassIndexManager.IndexChange indexChange : changes) {
      addIndexEntry(indexChange.index, indexChange.index.getName(), indexChange.operation, indexChange.key, indexChange.value);
    }
  }

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }
}
