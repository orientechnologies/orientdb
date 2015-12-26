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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;

import java.util.HashMap;
import java.util.Map;

public class OIntentMassiveInsert implements OIntent {
  private boolean                                     previousRetainRecords;
  private boolean                                     previousRetainObjects;
  private boolean                                     previousValidation;
  private boolean                                     previousTxRequiredForSQLGraphOperations;
  private Map<ORecordHook, ORecordHook.HOOK_POSITION> removedHooks;
  private OSecurityUser                               currentUser;
  private boolean                                     disableValidation = true;
  private boolean                                     disableSecurity   = true;
  private boolean                                     disableHooks      = true;
  private boolean                                     enableCache       = true;

  public void begin(final ODatabaseDocumentInternal iDatabase) {
    if (disableSecurity) {
      // DISABLE CHECK OF SECURITY
      currentUser = iDatabase.getDatabaseOwner().getUser();
      iDatabase.getDatabaseOwner().setUser(null);
    }
    ODatabaseInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    // DISABLE TX IN GRAPH SQL OPERATIONS
    previousTxRequiredForSQLGraphOperations = ownerDb.getStorage().getConfiguration().isTxRequiredForSQLGraphOperations();
    if (previousTxRequiredForSQLGraphOperations)
      ownerDb.getStorage().getConfiguration().setProperty("txRequiredForSQLGraphOperations", Boolean.FALSE.toString());

    if (!enableCache) {
      ownerDb.getLocalCache().setEnable(enableCache);
    }

    if (ownerDb instanceof ODatabaseDocument) {
      previousRetainRecords = ((ODatabaseDocument) ownerDb).isRetainRecords();
      ((ODatabaseDocument) ownerDb).setRetainRecords(false);

      // VALIDATION
      if (disableValidation) {
        previousValidation = ((ODatabaseDocument) ownerDb).isValidationEnabled();
        if (previousValidation)
          ((ODatabaseDocument) ownerDb).setValidationEnabled(false);
      }
    }

    while (ownerDb.getDatabaseOwner() != ownerDb)
      ownerDb = ownerDb.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseObject) {
      previousRetainObjects = ((ODatabaseObject) ownerDb).isRetainObjects();
      ((ODatabaseObject) ownerDb).setRetainObjects(false);
    }

    if (disableHooks) {
      // REMOVE ALL HOOKS BUT INDEX
      removedHooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>();
      HashMap<ORecordHook, ORecordHook.HOOK_POSITION> hooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>(
          ownerDb.getHooks());
      for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> hook : hooks.entrySet()) {
        if (!(hook.getKey() instanceof OClassIndexManager)) {
          removedHooks.put(hook.getKey(), hook.getValue());
          ownerDb.unregisterHook(hook.getKey());
        }
      }
    }
  }

  public void end(final ODatabaseDocumentInternal iDatabase) {
    ODatabaseInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (disableSecurity)
      if (currentUser != null)
        // RE-ENABLE CHECK OF SECURITY
        ownerDb.setUser(currentUser);

    if (previousTxRequiredForSQLGraphOperations)
      ownerDb.getStorage().getConfiguration().setProperty("txRequiredForSQLGraphOperations", Boolean.TRUE.toString());

    if (!enableCache) {
      ownerDb.getLocalCache().setEnable(!enableCache);
    }
    if (ownerDb instanceof ODatabaseDocument) {
      ((ODatabaseDocument) ownerDb).setRetainRecords(previousRetainRecords);
      if (disableValidation)
        ((ODatabaseDocument) ownerDb).setValidationEnabled(previousValidation);
    }

    while (ownerDb.getDatabaseOwner() != ownerDb)
      ownerDb = ownerDb.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseObject)
      ((ODatabaseObject) ownerDb).setRetainObjects(previousRetainObjects);

    if (disableHooks)
      if (removedHooks != null) {
        // RESTORE ALL REMOVED HOOKS
        for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> hook : removedHooks.entrySet()) {
          ownerDb.registerHook(hook.getKey(), hook.getValue());
        }
      }
  }

  public boolean isDisableValidation() {
    return disableValidation;
  }

  public OIntentMassiveInsert setDisableValidation(final boolean disableValidation) {
    this.disableValidation = disableValidation;
    return this;
  }

  public boolean isDisableSecurity() {
    return disableSecurity;
  }

  public OIntentMassiveInsert setDisableSecurity(final boolean disableSecurity) {
    this.disableSecurity = disableSecurity;
    return this;
  }

  public boolean isDisableHooks() {
    return disableHooks;
  }

  public OIntentMassiveInsert setDisableHooks(final boolean disableHooks) {
    this.disableHooks = disableHooks;
    return this;

  }

  public OIntentMassiveInsert setEnableCache(boolean enableCache) {
    this.enableCache = enableCache;
    return this;

  }

  @Override
  public OIntent copy() {
    final OIntentMassiveInsert copy = new OIntentMassiveInsert();
    copy.previousRetainRecords = previousRetainRecords;
    copy.previousRetainObjects = previousRetainObjects;
    copy.previousValidation = previousValidation;
    copy.disableValidation = disableValidation;
    copy.disableSecurity = disableSecurity;
    copy.disableHooks = disableHooks;
    copy.currentUser = currentUser;
    if (removedHooks != null)
      copy.removedHooks = new HashMap<ORecordHook, ORecordHook.HOOK_POSITION>(removedHooks);
    return copy;
  }
}
