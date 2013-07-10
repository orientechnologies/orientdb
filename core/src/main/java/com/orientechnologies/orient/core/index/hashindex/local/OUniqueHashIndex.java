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
package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * @author Andrey Lomakin
 * @since 18.02.13
 */
public class OUniqueHashIndex extends OAbstractLocalHashIndex<OIdentifiable> {
  public static final String TYPE_ID = OClass.INDEX_TYPE.UNIQUE_HASH.toString();

  public OUniqueHashIndex() {
    super(TYPE_ID);
  }

  @Override
  public OIndex<OIdentifiable> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase,
      String iClusterIndexName, int[] iClusterIdsToIndex, boolean rebuild, OProgressListener iProgressListener) {
    create(iName, iIndexDefinition, iDatabase, iClusterIndexName, iClusterIdsToIndex, rebuild, iProgressListener,
        OLinkSerializer.INSTANCE);
    return this;
  }

  @Override
  public long count(Object iKey) {
    if (get(iKey) != null)
      return 1;

    return 0;
  }

  @Override
  public boolean contains(Object iKey) {
    return get(iKey) != null;
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
          throw new ORecordDuplicatedException(String.format(
              "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", null,
              OIndexException.class, value.getIdentity(), key, getName(), currentValue.getIdentity()), currentValue.getIdentity());
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

      OLogManager.instance().exception(
          "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", null,
          OIndexException.class, iRecord.getIdentity(), iKey, getName(), indexedRID.getIdentity());
    }
  }
}
