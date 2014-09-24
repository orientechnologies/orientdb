package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseComplexInternal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.security.OUser;

import java.util.HashMap;
import java.util.Map;

public class OIntentMassiveInsert implements OIntent {
  private boolean                                     previousLocalCacheEnabled;
  private boolean                                     previousRetainRecords;
  private boolean                                     previousRetainObjects;
  private boolean                                     previousValidation;
  private Map<ORecordHook, ORecordHook.HOOK_POSITION> removedHooks;
  private OUser                                       currentUser;

  public void begin(final ODatabaseRaw iDatabase) {
    // DISABLE CHECK OF SECURITY
    currentUser = iDatabase.getDatabaseOwner().getUser();
    iDatabase.getDatabaseOwner().setUser(null);

    previousLocalCacheEnabled = iDatabase.getDatabaseOwner().getLocalCache().isEnabled();
    iDatabase.getDatabaseOwner().getLocalCache().setEnable(false);

    ODatabaseComplexInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseRecord) {
      previousRetainRecords = ((ODatabaseRecord) ownerDb).isRetainRecords();
      ((ODatabaseRecord) ownerDb).setRetainRecords(false);

      // VALIDATION
      previousValidation = ((ODatabaseRecord) ownerDb).isValidationEnabled();
      if (previousValidation)
        ((ODatabaseRecord) ownerDb).setValidationEnabled(false);
    }

    while (ownerDb.getDatabaseOwner() != ownerDb)
      ownerDb = ownerDb.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseObject) {
      previousRetainObjects = ((ODatabaseObject) ownerDb).isRetainObjects();
      ((ODatabaseObject) ownerDb).setRetainObjects(false);
    }

    // REMOVE ALL HOOKS BUT INDEX
    removedHooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>();
    HashMap<ORecordHook, ORecordHook.HOOK_POSITION> hooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>(ownerDb.getHooks());
    for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> hook : hooks.entrySet()) {
      if (!(hook.getKey() instanceof OClassIndexManager)) {
        removedHooks.put(hook.getKey(), hook.getValue());
        ownerDb.unregisterHook(hook.getKey());
      }
    }
  }

  public void end(final ODatabaseRaw iDatabase) {
    if (currentUser != null)
      // RE-ENABLE CHECK OF SECURITY
      iDatabase.getDatabaseOwner().setUser(currentUser);

    iDatabase.getDatabaseOwner().getLocalCache().setEnable(previousLocalCacheEnabled);
    ODatabaseComplexInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseRecord) {
      ((ODatabaseRecord) ownerDb).setRetainRecords(previousRetainRecords);
      ((ODatabaseRecord) ownerDb).setValidationEnabled(previousValidation);
    }

    while (ownerDb.getDatabaseOwner() != ownerDb)
      ownerDb = ownerDb.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseObject)
      ((ODatabaseObject) ownerDb).setRetainObjects(previousRetainObjects);

    if (removedHooks != null) {
      // RESTORE ALL REMOVED HOOKS
      for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> hook : removedHooks.entrySet()) {
        ownerDb.registerHook(hook.getKey(), hook.getValue());
      }
    }

  }

  @Override
  public OIntent copy() {
    final OIntentMassiveInsert copy = new OIntentMassiveInsert();
    copy.previousLocalCacheEnabled = previousLocalCacheEnabled;
    copy.previousRetainRecords = previousRetainRecords;
    copy.previousRetainObjects = previousRetainObjects;
    copy.previousValidation = previousValidation;
    copy.currentUser = currentUser;
    if (removedHooks != null)
      copy.removedHooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>(removedHooks);
    return copy;
  }
}
