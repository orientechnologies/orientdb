package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by tglman on 03/01/17.
 */
public class OTransactionOptimisticClient extends OTransactionOptimistic {
  public OTransactionOptimisticClient(ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
  }

  public void replaceContent(List<ORecordOperationRequest> operations, List<IndexChange> indexChanges) {

    this.allEntries.clear();

    for (ORecordOperationRequest operation : operations) {
      ORecordInternal.setIdentity(operation.getRecord(), (ORecordId) operation.getId());
      ORecordInternal.setVersion(operation.getRecord(), operation.getVersion());
      addRecord(operation.getRecord(), operation.getType(), null);
    }

    for (IndexChange change : indexChanges) {
      NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
      for (Map.Entry<Object, OTransactionIndexChangesPerKey> keyChange : change.getKeyChanges().changesPerKey.entrySet()) {
        Object key = keyChange.getKey();
        if (key instanceof OIdentifiable && ((OIdentifiable) key).getIdentity().isNew())
          key = ((OIdentifiable) key).getRecord();
        OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(key);
        singleChange.entries.addAll(keyChange.getValue().entries);
        changesPerKey.put(key, singleChange);
      }
      change.getKeyChanges().changesPerKey = changesPerKey;

      indexEntries.put(change.getName(), change.getKeyChanges());
    }
  }

}
