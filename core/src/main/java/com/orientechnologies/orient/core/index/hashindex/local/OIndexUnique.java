package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * @author Andrey Lomakin
 * @since 18.02.13
 */
public class OIndexUnique extends OAbstractLocalHashIndex<OIdentifiable> {
  public static final String TYPE_ID = OClass.INDEX_TYPE.UNIQUE_HASH.toString();

  public OIndexUnique() {
    super(TYPE_ID);
  }

  @Override
  public OIndex<OIdentifiable> put(Object key, OIdentifiable value) {
    acquireExclusiveLock();
    try {
      checkForKeyType(key);

      final OIdentifiable currentValue = super.get(key);

      if (currentValue != null) {
        // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
        if (!currentValue.equals(value))
          throw new ORecordDuplicatedException("Found duplicated key '" + key + "' on unique index '" + getName() + "' for record "
              + value.getIdentity() + ". The record already present in the index is " + currentValue.getIdentity(),
              currentValue.getIdentity());
        else
          return this;
      }

      if (!value.getIdentity().isPersistent())
        ((ORecord<?>) value.getRecord()).save();

      super.put(key, value.getIdentity());
      return this;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void checkEntry(OIdentifiable iRecord, Object iKey) {
    final OIdentifiable indexedRID = get(iKey);
    if (indexedRID != null && !indexedRID.getIdentity().equals(iRecord.getIdentity())) {
      // CHECK IF IN THE SAME TX THE ENTRY WAS DELETED
      final OTransactionIndexChanges indexChanges = ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
          .getIndexChanges(getName());
      if (indexChanges != null) {
        final OTransactionIndexChangesPerKey keyChanges = indexChanges.getChangesPerKey(iKey);
        if (keyChanges != null) {
          for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : keyChanges.entries) {
            if (entry.operation == OTransactionIndexChanges.OPERATION.REMOVE)
              // WAS DELETED, OK!
              return;
          }
        }
      }

      OLogManager.instance().exception("Found duplicated key '%s' previously assigned to the record %s", null,
          OIndexException.class, iKey, indexedRID);
    }
  }
}
