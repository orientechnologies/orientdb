package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;

import java.util.HashMap;
import java.util.Map;

public class OIntentMassiveInsert implements OIntent {
  private boolean                                     previousLevel1CacheEnabled;
  private boolean                                     previousLevel2CacheEnabled;
  private boolean                                     previousRetainRecords;
  private boolean                                     previousRetainObjects;
  private Map<ORecordHook, ORecordHook.HOOK_POSITION> removedHooks;

  public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
    previousLevel1CacheEnabled = iDatabase.getDatabaseOwner().getLevel1Cache().isEnabled();
    iDatabase.getDatabaseOwner().getLevel1Cache().setEnable(false);
    previousLevel2CacheEnabled = iDatabase.getDatabaseOwner().getLevel2Cache().isEnabled();
    iDatabase.getDatabaseOwner().getLevel2Cache().setEnable(false);

    ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseRecord) {
      previousRetainRecords = ((ODatabaseRecord) ownerDb).isRetainRecords();
      ((ODatabaseRecord) ownerDb).setRetainRecords(false);
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
    iDatabase.getDatabaseOwner().getLevel1Cache().setEnable(previousLevel1CacheEnabled);
    iDatabase.getDatabaseOwner().getLevel2Cache().setEnable(previousLevel2CacheEnabled);

    ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseRecord)
      ((ODatabaseRecord) ownerDb).setRetainRecords(previousRetainRecords);

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
}
