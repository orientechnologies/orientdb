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

package com.orientechnologies.orient.core.intent;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.ODatabaseComplexInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;

public class OIntentMassiveInsert implements OIntent {
  private boolean                                     previousRetainRecords;
  private boolean                                     previousRetainObjects;
  private boolean                                     previousValidation;
  private Map<ORecordHook, ORecordHook.HOOK_POSITION> removedHooks;
  private OSecurityUser                               currentUser;

  public void begin(final ODatabaseRecordInternal iDatabase) {
    // DISABLE CHECK OF SECURITY
    currentUser = iDatabase.getDatabaseOwner().getUser();
    iDatabase.getDatabaseOwner().setUser(null);

    ODatabaseComplexInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseDocument) {
      previousRetainRecords = ((ODatabaseDocument) ownerDb).isRetainRecords();
      ((ODatabaseDocument) ownerDb).setRetainRecords(false);

      // VALIDATION
      previousValidation = ((ODatabaseDocument) ownerDb).isValidationEnabled();
      if (previousValidation)
        ((ODatabaseDocument) ownerDb).setValidationEnabled(false);
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

  public void end(final ODatabaseRecordInternal iDatabase) {
    if (currentUser != null)
      // RE-ENABLE CHECK OF SECURITY
      iDatabase.getDatabaseOwner().setUser(currentUser);

    ODatabaseComplexInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseDocument) {
      ((ODatabaseDocument) ownerDb).setRetainRecords(previousRetainRecords);
      ((ODatabaseDocument) ownerDb).setValidationEnabled(previousValidation);
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
    copy.previousRetainRecords = previousRetainRecords;
    copy.previousRetainObjects = previousRetainObjects;
    copy.previousValidation = previousValidation;
    copy.currentUser = currentUser;
    if (removedHooks != null)
      copy.removedHooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>(removedHooks);
    return copy;
  }
}
